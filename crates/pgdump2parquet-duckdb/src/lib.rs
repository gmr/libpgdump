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
}

impl DuckDbFactory {
    pub fn new(opts: SinkOpts) -> Self {
        Self { opts }
    }
}

impl ParquetSinkFactory for DuckDbFactory {
    fn open(
        &self,
        out: &std::path::Path,
        schema: &TableSchema,
    ) -> Result<Box<dyn ParquetSink>, SinkError> {
        let conn = Connection::open_in_memory().map_err(boxed)?;
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
            batch_rows: self.opts.batch_rows,
            row_group_rows: self.opts.row_group_rows,
            zstd_level: self.opts.zstd_level,
            // Column-major staging: one Vec of `Option<String>` per column.
            // Keeping values grouped per column lets the appender stream
            // them down efficiently when we flush.
            buffered: (0..ncols).map(|_| Vec::with_capacity(8192)).collect(),
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
    batch_rows: usize,
    row_group_rows: usize,
    zstd_level: i32,
    /// Column-major row buffer. `buffered[c][r]` is the value for column `c`
    /// of row `r` within the current batch.
    buffered: Vec<Vec<Option<String>>>,
    total_rows: usize,
}

impl DuckDbSink {
    fn flush_buffer(&mut self) -> Result<(), SinkError> {
        let rows = self.buffered.first().map(|v| v.len()).unwrap_or(0);
        if rows == 0 {
            return Ok(());
        }
        let mut app: Appender<'_> = self.conn.appender("_stage").map_err(boxed)?;
        // Push each buffered row into the appender. We iterate row-first
        // because that's the Appender's natural shape, reusing a tiny
        // per-row param vector.
        let mut row_params: Vec<Option<&str>> = Vec::with_capacity(self.ncols);
        for r in 0..rows {
            row_params.clear();
            for c in 0..self.ncols {
                row_params.push(self.buffered[c][r].as_deref());
            }
            app.append_row(appender_params_from_iter(row_params.iter()))
                .map_err(boxed)?;
        }
        drop(app); // flushes

        // Clear buffers but keep capacity for the next batch.
        for col in &mut self.buffered {
            col.clear();
        }
        Ok(())
    }
}

impl ParquetSink for DuckDbSink {
    fn append_row(&mut self, fields: &[Option<&[u8]>]) -> Result<(), SinkError> {
        for i in 0..self.ncols {
            let v = fields.get(i).copied().flatten().map(|bytes| {
                match std::str::from_utf8(bytes) {
                    Ok(s) => s.to_string(),
                    Err(_) => String::from_utf8_lossy(bytes).into_owned(),
                }
            });
            self.buffered[i].push(v);
        }
        self.total_rows += 1;
        if self.buffered[0].len() >= self.batch_rows {
            self.flush_buffer()?;
        }
        Ok(())
    }

    fn close(mut self: Box<Self>) -> Result<SinkStats, SinkError> {
        self.flush_buffer()?;
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

fn quote_ident(s: &str) -> String {
    let escaped = s.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn sql_quote(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
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
