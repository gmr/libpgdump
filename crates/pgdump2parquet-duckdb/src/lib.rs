//! DuckDB-backed [`ParquetSink`] implementation for `pgdump2parquet`.
//!
//! Per-table flow:
//!
//! 1. Open an in-memory DuckDB.
//! 2. `CREATE TABLE _stage (col1 VARCHAR, col2 VARCHAR, ...)`.
//! 3. Rows arrive via [`ParquetSink::append_row`] and are pushed into
//!    `_stage` through DuckDB's [`Appender`] (the fastest bulk-insert path).
//! 4. On [`ParquetSink::close`], run
//!    `COPY (SELECT TRY_CAST(col AS <target>), ...) TO 'out.parquet'` so
//!    DuckDB does the CAST and Parquet encoding in one pass with its native
//!    optimisations (parallel row groups, dictionary encoding, zstd).
//!
//! Why stage as VARCHAR and `TRY_CAST` at export (rather than typing the
//! staging table up front): pg's COPY TEXT encoding doesn't always match
//! DuckDB's input conventions. `TRY_CAST` degrades unparsable values to
//! NULL instead of failing the whole export — an important property when
//! you're liberating a dump with imperfect DDL translation.

use duckdb::{Appender, Connection, appender_params_from_iter};

use pgdump2parquet_core::block::{ColumnBuffer, ColumnarBlock};
use pgdump2parquet_core::ddl::ColumnDef;
use pgdump2parquet_core::sink::{
    ParquetSink, ParquetSinkFactory, SinkError, SinkOpts, SinkStats, TableSchema,
};

/// Factory for DuckDB-backed sinks. Holds the user's [`SinkOpts`]; a fresh
/// in-memory DuckDB is created per table (DuckDB connections aren't shared
/// across threads and the sink is write-only, so there's nothing to reuse).
#[derive(Debug, Clone)]
pub struct DuckDbFactory {
    pub opts: SinkOpts,
    /// Per-connection `PRAGMA threads`. Defaults to 1 because the CLI runs
    /// N sinks in parallel — letting each DuckDB also fan out to all cores
    /// leads to N×M thread stampede and cache thrash.
    pub threads_per_sink: usize,
    /// Per-connection `PRAGMA memory_limit`. `None` = DuckDB's default
    /// (80% of system RAM), which is ruinous with N concurrent sinks.
    /// Something like `"2GB"` is usually right for parallel runs.
    pub memory_limit: Option<String>,
    /// Per-connection `PRAGMA temp_directory`. Used when DuckDB spills the
    /// staging table to disk. If `None`, DuckDB picks a default under
    /// CWD which can collide across concurrent sinks — the CLI plugs in a
    /// per-worker path here.
    pub temp_directory: Option<String>,
}

impl DuckDbFactory {
    pub fn new(opts: SinkOpts) -> Self {
        Self {
            opts,
            threads_per_sink: 1,
            memory_limit: None,
            temp_directory: None,
        }
    }

    /// Override the default `PRAGMA threads` for each DuckDB the factory
    /// creates. `0` means "DuckDB default" (= all cores).
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads_per_sink = threads;
        self
    }

    /// Set `PRAGMA memory_limit` on each DuckDB the factory creates. Accepts
    /// the usual DuckDB syntax, e.g. `"2GB"`, `"512MB"`.
    pub fn with_memory_limit(mut self, limit: impl Into<String>) -> Self {
        self.memory_limit = Some(limit.into());
        self
    }

    /// Set `PRAGMA temp_directory` on each DuckDB the factory creates.
    pub fn with_temp_directory(mut self, dir: impl Into<String>) -> Self {
        self.temp_directory = Some(dir.into());
        self
    }
}

impl ParquetSinkFactory for DuckDbFactory {
    fn open(
        &self,
        out: &std::path::Path,
        schema: &TableSchema,
    ) -> Result<Box<dyn ParquetSink>, SinkError> {
        let conn = Connection::open_in_memory().map_err(boxed)?;

        // Apply resource caps BEFORE we create the staging table. A
        // DuckDB-heavy laptop will happily light itself on fire if we don't.
        if self.threads_per_sink > 0 {
            conn.execute_batch(&format!("PRAGMA threads={};", self.threads_per_sink))
                .map_err(boxed)?;
        }
        if let Some(ref lim) = self.memory_limit {
            conn.execute_batch(&format!("PRAGMA memory_limit='{}';", sql_escape(lim)))
                .map_err(boxed)?;
        }
        if let Some(ref td) = self.temp_directory {
            conn.execute_batch(&format!("PRAGMA temp_directory='{}';", sql_escape(td)))
                .map_err(boxed)?;
        }

        let stage_cols = schema
            .columns
            .iter()
            .map(|c| format!("{} VARCHAR", quote_ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!("CREATE TABLE _stage ({stage_cols});"))
            .map_err(boxed)?;

        let ncols = schema.columns.len();
        Ok(Box::new(DuckDbSink {
            conn,
            cols: schema.columns.clone(),
            out_path: out.to_path_buf(),
            row_group_rows: self.opts.row_group_rows,
            zstd_level: self.opts.zstd_level,
            ncols,
            total_rows: 0,
        }))
    }
}

/// Map a pg type string (as it appears in a `CREATE TABLE`) to the DuckDB
/// type we'll cast to at export time. Anything we don't recognise stays as
/// `VARCHAR` so the value is still preserved losslessly.
pub fn pg_to_duckdb_type(pg_type: &str) -> String {
    let t = pg_type.trim();
    if t.ends_with("[]") || t.starts_with("ARRAY") || t.contains(" ARRAY") {
        return "VARCHAR".into();
    }

    let lower = t.to_ascii_lowercase();
    let (base, paren) = match lower.find('(') {
        Some(i) => {
            let close = lower[i..]
                .find(')')
                .map(|j| i + j + 1)
                .unwrap_or(lower.len());
            (lower[..i].trim(), Some(&lower[i..close]))
        }
        None => (lower.as_str(), None),
    };

    match base {
        "smallint" | "int2" => "SMALLINT".into(),
        "integer" | "int" | "int4" => "INTEGER".into(),
        "bigint" | "int8" => "BIGINT".into(),
        "real" | "float4" => "REAL".into(),
        "double precision" | "float8" => "DOUBLE".into(),
        "numeric" | "decimal" => match paren {
            Some(p) => format!("DECIMAL{p}"),
            None => "DECIMAL(38,10)".into(),
        },
        "boolean" | "bool" => "BOOLEAN".into(),
        "date" => "DATE".into(),
        "time" | "time without time zone" => "TIME".into(),
        "time with time zone" | "timetz" => "TIME".into(),
        "timestamp" | "timestamp without time zone" => "TIMESTAMP".into(),
        "timestamp with time zone" | "timestamptz" => "TIMESTAMPTZ".into(),
        "uuid" => "UUID".into(),
        "json" | "jsonb" => "JSON".into(),
        _ => "VARCHAR".into(),
    }
}

struct DuckDbSink {
    conn: Connection,
    cols: Vec<ColumnDef>,
    ncols: usize,
    out_path: std::path::PathBuf,
    row_group_rows: usize,
    zstd_level: i32,
    total_rows: usize,
}

impl ParquetSink for DuckDbSink {
    fn append_block(&mut self, block: ColumnarBlock) -> Result<(), SinkError> {
        if block.n_rows == 0 {
            return Ok(());
        }
        if block.columns.len() != self.ncols {
            return Err(boxed(std::io::Error::other(format!(
                "column count mismatch: block has {}, schema has {}",
                block.columns.len(),
                self.ncols
            ))));
        }

        // Build one column-slice view per column so we can index by row
        // without allocating per-row Options. `field` returns `Option<&str>`
        // straight out of the Arrow-shaped buffers. The block owns the
        // underlying Vecs for the duration of this call.
        let n_rows = block.n_rows;
        let views: Vec<ColumnView<'_>> = block
            .columns
            .iter()
            .map(|c| ColumnView::from(c, n_rows))
            .collect::<Result<_, SinkError>>()?;

        let mut app: Appender<'_> = self.conn.appender("_stage").map_err(boxed)?;
        let mut row_params: Vec<Option<&str>> = Vec::with_capacity(self.ncols);
        for r in 0..n_rows {
            row_params.clear();
            for v in &views {
                row_params.push(v.field(r));
            }
            app.append_row(appender_params_from_iter(row_params.iter()))
                .map_err(boxed)?;
        }
        drop(app);

        self.total_rows += n_rows;
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<SinkStats, SinkError> {
        let select_list = self
            .cols
            .iter()
            .map(|c| {
                let dt = pg_to_duckdb_type(&c.pg_type);
                if dt == "VARCHAR" {
                    quote_ident(&c.name)
                } else {
                    format!(
                        "TRY_CAST({} AS {}) AS {}",
                        quote_ident(&c.name),
                        dt,
                        quote_ident(&c.name)
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let copy_sql = format!(
            "COPY (SELECT {select_list} FROM _stage) TO {} \
             (FORMAT PARQUET, COMPRESSION 'zstd', COMPRESSION_LEVEL {}, ROW_GROUP_SIZE {});",
            sql_quote(self.out_path.to_string_lossy().as_ref()),
            self.zstd_level,
            self.row_group_rows,
        );
        self.conn.execute_batch(&copy_sql).map_err(boxed)?;
        Ok(SinkStats {
            rows_written: self.total_rows,
        })
    }
}

/// Zero-allocation row access over a block column. Resolves NULLs via the
/// packed validity bitmap and slices into the Arrow-shaped values buffer
/// using the offsets array. UTF-8 validation is done per-value by the
/// DuckDB appender's binding layer, so we pass raw `&str` where we can;
/// non-UTF-8 bytes fall back to a lossy conversion cached for the lifetime
/// of the column view.
struct ColumnView<'a> {
    n_rows: usize,
    values: &'a [u8],
    offsets: &'a [i32],
    validity: Option<&'a [u8]>,
    /// Lazily populated when a non-UTF-8 value is encountered.
    owned_strings: std::cell::UnsafeCell<Vec<Option<String>>>,
}

impl<'a> ColumnView<'a> {
    fn from(col: &'a ColumnBuffer, n_rows: usize) -> Result<Self, SinkError> {
        if col.offsets.len() != n_rows + 1 {
            return Err(boxed(std::io::Error::other(format!(
                "offsets length {} != n_rows+1 ({})",
                col.offsets.len(),
                n_rows + 1
            ))));
        }
        Ok(ColumnView {
            n_rows,
            values: &col.values,
            offsets: &col.offsets,
            validity: col.validity.as_deref(),
            owned_strings: std::cell::UnsafeCell::new(Vec::new()),
        })
    }

    fn is_null(&self, row: usize) -> bool {
        match self.validity {
            None => false,
            Some(bits) => bits[row / 8] & (1u8 << (row % 8)) == 0,
        }
    }

    fn field(&self, row: usize) -> Option<&str> {
        if row >= self.n_rows || self.is_null(row) {
            return None;
        }
        let start = self.offsets[row] as usize;
        let end = self.offsets[row + 1] as usize;
        let bytes = &self.values[start..end];
        match std::str::from_utf8(bytes) {
            Ok(s) => Some(s),
            Err(_) => {
                // Lazily materialise a lossy owned copy for this row.
                // SAFETY: we never hand out two mutable refs to the Vec
                // (`UnsafeCell` is only touched here, and the returned
                // reference is to a string inside it that is never removed).
                unsafe {
                    let v = &mut *self.owned_strings.get();
                    if v.len() < self.n_rows {
                        v.resize(self.n_rows, None);
                    }
                    if v[row].is_none() {
                        v[row] = Some(String::from_utf8_lossy(bytes).into_owned());
                    }
                    v[row].as_deref()
                }
            }
        }
    }
}

// SAFETY: the `UnsafeCell` is only mutated from `&self` in a way that
// never produces overlapping borrows (each `field` call returns a borrow
// into a stable slot). The sink is also `!Sync` in practice because it
// holds a DuckDB `Connection` that isn't Sync; we assert Send only.
unsafe impl Send for ColumnView<'_> {}

fn quote_ident(s: &str) -> String {
    let escaped = s.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn sql_quote(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

/// Escape a string to go inside single quotes in a PRAGMA. Mirrors
/// `sql_quote` but returns the *inside* of the quotes so the caller can
/// interpolate without nesting formats.
fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn boxed<E: std::error::Error + Send + Sync + 'static>(e: E) -> SinkError {
    Box::new(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_mapping() {
        assert_eq!(pg_to_duckdb_type("integer"), "INTEGER");
        assert_eq!(pg_to_duckdb_type("bigint"), "BIGINT");
        assert_eq!(pg_to_duckdb_type("text"), "VARCHAR");
        assert_eq!(pg_to_duckdb_type("character varying(64)"), "VARCHAR");
        assert_eq!(pg_to_duckdb_type("numeric(10,2)"), "DECIMAL(10,2)");
        assert_eq!(pg_to_duckdb_type("numeric"), "DECIMAL(38,10)");
        assert_eq!(pg_to_duckdb_type("boolean"), "BOOLEAN");
        assert_eq!(pg_to_duckdb_type("timestamp with time zone"), "TIMESTAMPTZ");
        assert_eq!(pg_to_duckdb_type("timestamp without time zone"), "TIMESTAMP");
        assert_eq!(pg_to_duckdb_type("jsonb"), "JSON");
        assert_eq!(pg_to_duckdb_type("uuid"), "UUID");
        assert_eq!(pg_to_duckdb_type("text[]"), "VARCHAR");
    }
}
