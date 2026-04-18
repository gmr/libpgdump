//! DuckDB-backed Parquet export path for the `pgdump2parquet` CLI.
//!
//! This module is only compiled when the `cli-duckdb` feature is enabled.
//! The Rust-only export path in `src/bin/pgdump2parquet.rs` is always
//! available; this one swaps out the arrow-rs Parquet writer for an
//! in-process DuckDB and additionally promotes VARCHAR columns to typed
//! columns using information parsed from the `CREATE TABLE` DDL.
//!
//! Architecture:
//!   1. Open an in-memory DuckDB.
//!   2. `CREATE TABLE _stage (col1 VARCHAR, col2 VARCHAR, ...)` — a flat
//!      staging table that matches the pg COPY text shape.
//!   3. Stream COPY rows from the pg dump via `CustomReader::read_entry_stream`,
//!      parse each row with [`crate::copy_text::parse_line`], and push into
//!      `_stage` through DuckDB's `Appender` (the fastest bulk-insert path).
//!   4. `COPY (SELECT TRY_CAST(col1 AS <target>), ...) TO 'out.parquet'` —
//!      DuckDB does the CAST + parquet encoding in a single pass with all
//!      its native optimisations (dictionary encoding, parallel row groups,
//!      zstd compression).
//!
//! Why staging as VARCHAR and CAST on export (instead of typing the staging
//! table up front): pg COPY emits values using pg's own text encoding, which
//! doesn't always match DuckDB's input conventions. `TRY_CAST` gives us
//! graceful degradation — a value DuckDB can't parse becomes `NULL` rather
//! than failing the whole row. Importantly, we stash the raw string in a
//! sibling column when the cast fails (not implemented yet, but the shape
//! supports it).

use std::io::{BufRead, BufReader, Read};

use duckdb::{Appender, Connection, appender_params_from_iter};

use crate::copy_text;
use crate::ddl::ColumnDef;
use crate::format::custom::CustomReader;

/// Map a pg type string (as it appears in a `CREATE TABLE`) to the DuckDB
/// type we'll cast to at export time. Anything we don't recognise stays as
/// `VARCHAR` so the value is still preserved losslessly.
pub fn pg_to_duckdb_type(pg_type: &str) -> String {
    let t = pg_type.trim();
    // Array types: keep as VARCHAR — pg emits `{a,b,c}` literals that don't
    // map cleanly to DuckDB's LIST literals without a custom parser.
    if t.ends_with("[]") || t.starts_with("ARRAY") || t.contains(" ARRAY") {
        return "VARCHAR".into();
    }

    let lower = t.to_ascii_lowercase();
    // Peel off a parenthesised modifier like `(64)` or `(10,2)` for matching.
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
        // Everything else: preserve as text (bytea included — its
        // `\xHEX` representation would need a dedicated decoder to become
        // a BLOB, which is out of scope for this MVP).
        _ => "VARCHAR".into(),
    }
}

/// Stream one TABLE DATA entry from a custom-format dump into a Parquet file
/// via an embedded DuckDB. Returns the row count written.
pub fn convert_table_duckdb<R: Read + std::io::Seek>(
    reader: &mut CustomReader<R>,
    dump_id: i32,
    cols: &[ColumnDef],
    out_path: &std::path::Path,
    batch_rows: usize,
    row_group_rows: usize,
    zstd_level: i32,
) -> anyhow::Result<usize> {
    let conn = Connection::open_in_memory()?;
    // A staging table with all-VARCHAR columns. Quote every identifier so
    // user column names like `group` or `order` don't collide with SQL
    // keywords. We keep the original pg column names verbatim.
    let stage_cols = cols
        .iter()
        .map(|c| format!("{} VARCHAR", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!("CREATE TABLE _stage ({stage_cols});"))?;

    // Stream rows.
    let Some(stream) = reader.read_entry_stream(dump_id)? else {
        return Ok(0);
    };
    let mut lines = BufReader::new(stream);
    let total_rows = append_copy_stream(&conn, &mut lines, cols.len(), batch_rows)?;

    // Build the final SELECT that CASTs each column to its DuckDB target
    // type. Using TRY_CAST so a weird value degrades to NULL rather than
    // failing the entire export — an important property when you're
    // liberating an archive without a perfect DDL translation.
    let select_list = cols
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

    // DuckDB's COPY ... TO 'path' (FORMAT PARQUET) is parallel and writes
    // row-groups as it goes — memory stays bounded.
    let copy_sql = format!(
        "COPY (SELECT {select_list} FROM _stage) TO {} \
         (FORMAT PARQUET, COMPRESSION 'zstd', COMPRESSION_LEVEL {zstd_level}, ROW_GROUP_SIZE {row_group_rows});",
        sql_quote(out_path.to_string_lossy().as_ref()),
    );
    conn.execute_batch(&copy_sql)?;

    Ok(total_rows)
}

/// Pull rows out of a COPY TEXT stream and push them into `_stage` via
/// DuckDB's appender. Returns the row count.
fn append_copy_stream<B: BufRead>(
    conn: &Connection,
    lines: &mut B,
    ncols: usize,
    batch_rows: usize,
) -> anyhow::Result<usize> {
    let mut app: Appender<'_> = conn.appender("_stage")?;
    let mut total = 0usize;
    let mut since_flush = 0usize;
    let mut line: Vec<u8> = Vec::new();

    loop {
        line.clear();
        let n = lines.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        if line == b"\\." {
            break;
        }

        let parsed = copy_text::parse_line(&line);
        // Coerce into exactly `ncols` string/null parameters.
        let mut params: Vec<Option<String>> = Vec::with_capacity(ncols);
        for i in 0..ncols {
            match parsed.get(i) {
                Some(Some(bytes)) => {
                    let s = match std::str::from_utf8(bytes) {
                        Ok(s) => s.to_string(),
                        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
                    };
                    params.push(Some(s));
                }
                _ => params.push(None),
            }
        }
        app.append_row(appender_params_from_iter(params.iter()))?;
        total += 1;
        since_flush += 1;
        if since_flush >= batch_rows {
            app.flush()?;
            since_flush = 0;
        }
    }
    app.flush()?;
    drop(app);
    Ok(total)
}

fn quote_ident(s: &str) -> String {
    let escaped = s.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn sql_quote(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
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
