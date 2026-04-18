//! DuckDB-backed [`ParquetSink`] implementation for `pgdump2parquet`.
//!
//! The core driver now hands us a typed Arrow `RecordBatch` per block
//! (schema derived from the parsed DDL), so this sink is a thin wrapper:
//!
//! 1. `CREATE TABLE _stage (<typed DDL>)` inside an in-memory DuckDB. The
//!    types come from [`pg_to_duckdb_type`], which mirrors the Arrow-side
//!    [`pgdump2parquet_core::typed::pg_to_arrow_type`] mapping.
//! 2. Rows arrive via [`ParquetSink::append_batch`] and are pushed into
//!    `_stage` as a whole chunk via DuckDB's `Appender::append_record_batch`
//!    (one FFI call per batch, not per row).
//! 3. On [`ParquetSink::close`], `COPY _stage TO 'out.parquet'` — no
//!    `TRY_CAST` rewrite, the staging table is already typed.
//!
//! Why keep a staging table at all: streaming an Arrow scan straight into
//! a `COPY (SELECT ... FROM arrow_scan(...)) TO ...` avoids the stage, but
//! needs a VTab-backed multi-batch producer that duckdb-rs 1.2.2 doesn't
//! expose directly. That's the next refactor; for now the stage is typed
//! and the append path is chunk-at-a-time, which already beats the old
//! row-at-a-time VARCHAR path on wide tables.

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use duckdb::{Appender, Connection};

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

    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads_per_sink = threads;
        self
    }

    pub fn with_memory_limit(mut self, limit: impl Into<String>) -> Self {
        self.memory_limit = Some(limit.into());
        self
    }

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

        // Typed staging DDL, driven off the same pg type strings the Arrow
        // schema used. For any DuckDB type we can't produce from a bare
        // Arrow type we fall back to VARCHAR — matches the Arrow sink's
        // Utf8 fallback and keeps the append compatible.
        let stage_cols = schema
            .columns
            .iter()
            .zip(schema.arrow_schema.fields().iter())
            .map(|(c, f)| {
                let ty = duckdb_type_for(c, f.data_type());
                format!("{} {}", quote_ident(&c.name), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!("CREATE TABLE _stage ({stage_cols});"))
            .map_err(boxed)?;

        Ok(Box::new(DuckDbSink {
            conn,
            out_path: out.to_path_buf(),
            row_group_rows: self.opts.row_group_rows,
            zstd_level: self.opts.zstd_level,
            total_rows: 0,
        }))
    }
}

/// Pick a DuckDB column type given (a) the pg type string from the DDL and
/// (b) the Arrow type the core driver decided to emit. We defer to the
/// Arrow decision for anything it typed, and use pg-level knowledge to
/// type a few things Arrow rendered as Utf8 (e.g. UUID, JSON). Anything
/// still ambiguous stays VARCHAR — `append_record_batch` will store the
/// Arrow `Utf8` value as-is.
fn duckdb_type_for(col: &ColumnDef, arrow_type: &DataType) -> String {
    match arrow_type {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Timestamp(_, None) => "TIMESTAMP".into(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ".into(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        DataType::Utf8 | DataType::LargeUtf8 => pg_to_duckdb_varchar_fallback(&col.pg_type),
        _ => "VARCHAR".into(),
    }
}

/// For Arrow `Utf8` columns, pick a DuckDB type that can still ingest the
/// value via the `VARCHAR` codepath but preserves intent where it helps.
/// Today: keep everything as VARCHAR — typed-cast of UUID/JSON/array
/// values from a string would fail on malformed rows and we want the
/// liberal, `TRY_CAST`-like behavior the rest of the pipeline has.
fn pg_to_duckdb_varchar_fallback(_pg_type: &str) -> String {
    "VARCHAR".into()
}

/// Legacy public helper — retained so the existing unit tests and any
/// out-of-tree consumers keep working. Prefer [`duckdb_type_for`] inside
/// this crate.
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
    out_path: std::path::PathBuf,
    row_group_rows: usize,
    zstd_level: i32,
    total_rows: usize,
}

impl ParquetSink for DuckDbSink {
    fn append_batch(&mut self, batch: RecordBatch) -> Result<(), SinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let n = batch.num_rows();
        let mut app: Appender<'_> = self.conn.appender("_stage").map_err(boxed)?;
        app.append_record_batch(batch).map_err(boxed)?;
        drop(app);
        self.total_rows += n;
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<SinkStats, SinkError> {
        let copy_sql = format!(
            "COPY _stage TO {} (FORMAT PARQUET, COMPRESSION 'zstd', COMPRESSION_LEVEL {}, ROW_GROUP_SIZE {});",
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

fn quote_ident(s: &str) -> String {
    let escaped = s.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn sql_quote(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

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
