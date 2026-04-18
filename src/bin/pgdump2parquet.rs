//! `pgdump2parquet` — convert a PostgreSQL custom-format dump (`pg_dump -Fc`)
//! directly to Parquet, one file per table, without going through Postgres.
//!
//! Design (see README section): the library exposes a streaming per-entry
//! reader over the dump's TOC, so we never materialise the full archive in
//! memory. Each `TABLE DATA` entry is streamed through a COPY-text parser
//! into Arrow `RecordBatch`es, then written to a Parquet file via
//! `parquet::arrow::ArrowWriter`.
//!
//! MVP type strategy: every column is written as Parquet `BYTE_ARRAY` (UTF-8
//! string). This keeps the converter small and correct against all the
//! quirky pg types (arrays, ranges, jsonb, composite types, enums, domains,
//! timestamps-with-timezone-with-microseconds…) — downstream tooling
//! (DuckDB's `TRY_CAST` etc.) is very good at coercing typed columns from
//! string parquet once you know your schema. A future pass can add type
//! inference from the CREATE TABLE DDL.

use std::fs::{File, create_dir_all};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{RecordBatch, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use libpgdump::copy_text;
use libpgdump::ddl;
use libpgdump::format::custom::CustomReader;
use libpgdump::types::ObjectType;

#[derive(clap::ValueEnum, Clone, Debug)]
enum Engine {
    /// Pure-Rust backend: arrow-rs + parquet crate. Every column is written
    /// as Parquet VARCHAR; cast downstream with DuckDB's `TRY_CAST`.
    Rust,
    /// Embedded DuckDB (feature `cli-duckdb`). Stages rows as VARCHAR, then
    /// uses DuckDB's `COPY ... TO 'x.parquet'` with `TRY_CAST` per column to
    /// emit typed Parquet in a single pass.
    Duckdb,
}

#[derive(Parser, Debug)]
#[command(
    name = "pgdump2parquet",
    about = "Convert a pg_dump custom-format archive directly to Parquet files (one per table).",
    version
)]
struct Cli {
    /// Path to the pg_dump `-Fc` archive.
    dump: PathBuf,
    /// Output directory (one `<schema>.<table>.parquet` file is written per
    /// table). Created if it doesn't already exist.
    #[arg(short, long, default_value = "out")]
    out_dir: PathBuf,
    /// Only convert tables matching this `schema.table` glob (repeatable).
    /// If no patterns are given, every table in the dump is converted.
    #[arg(short = 't', long = "table")]
    tables: Vec<String>,
    /// Export backend.
    #[arg(long, value_enum, default_value_t = Engine::Rust)]
    engine: Engine,
    /// Rows per Arrow `RecordBatch` flushed to the Parquet writer. Tune this
    /// for very wide tables: smaller batches use less memory, larger batches
    /// amortise Parquet encoding overhead.
    #[arg(long, default_value_t = 65_536)]
    batch_rows: usize,
    /// Rows per Parquet row group. DuckDB reads row groups in parallel, so
    /// bigger row groups give better compression but coarser parallelism.
    #[arg(long, default_value_t = 1_000_000)]
    row_group_rows: usize,
    /// Parquet compression level for zstd (1–22).
    #[arg(long, default_value_t = 3)]
    zstd_level: i32,
    /// List tables found in the dump and exit.
    #[arg(long)]
    list: bool,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let file = File::open(&cli.dump)
        .map_err(|e| anyhow::anyhow!("opening {}: {e}", cli.dump.display()))?;
    let mut reader = CustomReader::open(BufReader::new(file))?;

    // Build schema map: (namespace, tag) -> columns, from TABLE entries.
    let mut schemas: std::collections::HashMap<(String, String), Vec<ddl::ColumnDef>> =
        std::collections::HashMap::new();
    for e in reader.entries() {
        if e.desc != ObjectType::Table {
            continue;
        }
        let (Some(ns), Some(tag), Some(defn)) = (&e.namespace, &e.tag, &e.defn) else {
            continue;
        };
        if let Some(cols) = ddl::parse_create_table(defn) {
            schemas.insert((ns.clone(), tag.clone()), cols);
        }
    }

    // Collect TABLE DATA entries to process.
    let table_data: Vec<(i32, String, String)> = reader
        .entries()
        .iter()
        .filter(|e| e.desc == ObjectType::TableData && e.had_dumper)
        .map(|e| {
            (
                e.dump_id,
                e.namespace.clone().unwrap_or_default(),
                e.tag.clone().unwrap_or_default(),
            )
        })
        .filter(|(_, ns, tag)| {
            cli.tables.is_empty()
                || cli
                    .tables
                    .iter()
                    .any(|pat| matches_simple_glob(pat, &format!("{ns}.{tag}")))
        })
        .collect();

    if cli.list {
        println!("{} table(s) with data:", table_data.len());
        for (id, ns, tag) in &table_data {
            let ncols = schemas
                .get(&(ns.clone(), tag.clone()))
                .map(|v| v.len())
                .unwrap_or(0);
            println!("  #{id:>4} {ns}.{tag} ({ncols} cols)");
        }
        return Ok(());
    }

    create_dir_all(&cli.out_dir)?;

    let mut totals = (0usize, 0u64); // (tables, rows)
    for (dump_id, ns, tag) in table_data {
        let cols = match schemas.get(&(ns.clone(), tag.clone())) {
            Some(c) => c.clone(),
            None => {
                eprintln!("warn: no CREATE TABLE found for {ns}.{tag} (dump_id={dump_id}); skipping");
                continue;
            }
        };

        let out_path = cli
            .out_dir
            .join(format!("{}.{}.parquet", sanitize(&ns), sanitize(&tag)));
        eprint!("→ {ns}.{tag}  ({} cols)  ", cols.len());
        let rows = match cli.engine {
            Engine::Rust => convert_table(
                &mut reader,
                dump_id,
                &cols,
                &out_path,
                cli.batch_rows,
                cli.row_group_rows,
                cli.zstd_level,
            )?,
            Engine::Duckdb => convert_table_duckdb(
                &mut reader,
                dump_id,
                &cols,
                &out_path,
                cli.batch_rows,
                cli.row_group_rows,
                cli.zstd_level,
            )?,
        };
        eprintln!("{rows} rows → {}", out_path.display());
        totals.0 += 1;
        totals.1 += rows as u64;
    }

    eprintln!(
        "\nconverted {} table(s), {} total rows",
        totals.0, totals.1
    );
    Ok(())
}

fn convert_table<R: std::io::Read + std::io::Seek>(
    reader: &mut CustomReader<R>,
    dump_id: i32,
    cols: &[ddl::ColumnDef],
    out_path: &std::path::Path,
    batch_rows: usize,
    row_group_rows: usize,
    zstd_level: i32,
) -> anyhow::Result<usize> {
    // Build an all-strings Arrow schema for this table.
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| Field::new(&c.name, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(zstd_level)?))
        .set_max_row_group_size(row_group_rows)
        .build();

    let out_file = File::create(out_path)?;
    let mut writer = ArrowWriter::try_new(out_file, schema.clone(), Some(props))?;

    let Some(stream) = reader.read_entry_stream(dump_id)? else {
        writer.close()?;
        return Ok(0);
    };
    let mut buf = BufReader::new(stream);

    let mut builders: Vec<StringBuilder> = cols.iter().map(|_| StringBuilder::new()).collect();
    let mut pending: usize = 0;
    let mut total_rows: usize = 0;

    let mut line: Vec<u8> = Vec::new();
    loop {
        line.clear();
        let n = buf.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        // Strip trailing LF (and optional CR).
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
            // COPY end-of-data marker.
            break;
        }

        let fields = copy_text::parse_line(&line);
        // pad/truncate to the declared column count so row shape matches schema
        for (i, builder) in builders.iter_mut().enumerate() {
            match fields.get(i) {
                Some(Some(bytes)) => {
                    // pg COPY is supposed to be the DB's client encoding (UTF-8
                    // in the overwhelming majority of dumps). Use lossy conversion
                    // so we never fail on a stray non-UTF-8 byte (rare, but
                    // real for dumps from databases with mismatched encodings).
                    let s = match std::str::from_utf8(bytes) {
                        Ok(s) => std::borrow::Cow::Borrowed(s),
                        Err(_) => String::from_utf8_lossy(bytes),
                    };
                    builder.append_value(s.as_ref());
                }
                Some(None) | None => builder.append_null(),
            }
        }
        pending += 1;
        total_rows += 1;

        if pending >= batch_rows {
            flush_batch(&mut writer, &schema, &mut builders)?;
            pending = 0;
        }
    }
    if pending > 0 {
        flush_batch(&mut writer, &schema, &mut builders)?;
    }

    writer.close()?;
    Ok(total_rows)
}

fn flush_batch(
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
    builders: &mut [StringBuilder],
) -> anyhow::Result<()> {
    let arrays: Vec<Arc<dyn arrow_array::Array>> = builders
        .iter_mut()
        .map(|b| Arc::new(b.finish()) as Arc<dyn arrow_array::Array>)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    writer.write(&batch)?;
    Ok(())
}

#[cfg(feature = "cli-duckdb")]
use libpgdump::duckdb_export::convert_table_duckdb;

#[cfg(not(feature = "cli-duckdb"))]
fn convert_table_duckdb<R: std::io::Read + std::io::Seek>(
    _reader: &mut CustomReader<R>,
    _dump_id: i32,
    _cols: &[ddl::ColumnDef],
    _out_path: &std::path::Path,
    _batch_rows: usize,
    _row_group_rows: usize,
    _zstd_level: i32,
) -> anyhow::Result<usize> {
    anyhow::bail!(
        "the `--engine duckdb` backend requires building with the `cli-duckdb` feature: \
         cargo install --path . --features cli-duckdb"
    )
}

/// Very small glob matcher: supports `*` (matches any number of chars, inc.
/// `.`) and `?` (matches one char). Good enough for `public.*`, `*.foo`, etc.
fn matches_simple_glob(pat: &str, s: &str) -> bool {
    fn inner(p: &[u8], s: &[u8]) -> bool {
        match (p.first(), s.first()) {
            (None, None) => true,
            (Some(&b'*'), _) => {
                if inner(&p[1..], s) {
                    return true;
                }
                if s.is_empty() {
                    return false;
                }
                inner(p, &s[1..])
            }
            (Some(&b'?'), Some(_)) => inner(&p[1..], &s[1..]),
            (Some(pc), Some(sc)) if pc == sc => inner(&p[1..], &s[1..]),
            _ => false,
        }
    }
    inner(pat.as_bytes(), s.as_bytes())
}

/// Sanitize a pg identifier for use as a filename component.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect()
}
