//! `pgdump2parquet` — convert a PostgreSQL custom-format dump (`pg_dump -Fc`)
//! directly to Parquet, one file per table, without going through Postgres.
//!
//! This crate is a thin CLI dispatcher. The actual work lives in:
//!
//! * `pgdump2parquet-core`  — COPY/DDL parsers, sink trait, per-table driver.
//! * `pgdump2parquet-arrow` — arrow-rs + parquet-crate sink (all-strings).
//! * `pgdump2parquet-duckdb` — embedded DuckDB sink (typed columns).
//!
//! The `--engine` flag picks which sink factory to instantiate. The core
//! driver is engine-agnostic and only sees the `ParquetSink` trait.

use std::collections::HashMap;
use std::fs::{File, create_dir_all};
use std::io::BufReader;
use std::path::PathBuf;

use clap::Parser;
use libpgdump::format::custom::CustomReader;
use libpgdump::types::ObjectType;
use pgdump2parquet_core::sink::{ParquetSinkFactory, SinkOpts, TableSchema};
use pgdump2parquet_core::{ddl, drive_table};

#[derive(clap::ValueEnum, Clone, Debug, PartialEq, Eq)]
enum Engine {
    /// Pure-Rust backend: arrow-rs + parquet crate. Every column is written
    /// as Parquet VARCHAR; cast downstream with DuckDB's `TRY_CAST`.
    Rust,
    /// Embedded DuckDB (feature `duckdb`). Stages rows as VARCHAR, then
    /// uses `COPY ... TO 'x.parquet'` with `TRY_CAST` per column to emit
    /// typed Parquet in a single pass.
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
    /// Rows per internal batch flush.
    #[arg(long, default_value_t = 65_536)]
    batch_rows: usize,
    /// Rows per Parquet row group.
    #[arg(long, default_value_t = 1_000_000)]
    row_group_rows: usize,
    /// Parquet zstd compression level (1–22).
    #[arg(long, default_value_t = 3)]
    zstd_level: i32,
    /// List tables found in the dump and exit.
    #[arg(long)]
    list: bool,
    /// Skip tables whose output `*.parquet` file already exists. Useful for
    /// resumability — re-run the same command after a crash and only the
    /// tables that didn't finish will be processed.
    #[arg(long)]
    skip_existing: bool,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let file = File::open(&cli.dump)
        .map_err(|e| anyhow::anyhow!("opening {}: {e}", cli.dump.display()))?;
    let mut reader = CustomReader::open(BufReader::new(file))?;

    // Build schema map: (namespace, tag) -> columns.
    let mut schemas: HashMap<(String, String), Vec<ddl::ColumnDef>> = HashMap::new();
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

    // Collect TABLE DATA entries, filtered by the user's --table globs.
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

    let opts = SinkOpts {
        batch_rows: cli.batch_rows,
        row_group_rows: cli.row_group_rows,
        zstd_level: cli.zstd_level,
    };
    let factory = make_factory(cli.engine.clone(), opts)?;

    let mut totals = (0usize, 0u64);
    for (dump_id, ns, tag) in table_data {
        let cols = match schemas.get(&(ns.clone(), tag.clone())) {
            Some(c) => c.clone(),
            None => {
                eprintln!(
                    "warn: no CREATE TABLE found for {ns}.{tag} (dump_id={dump_id}); skipping"
                );
                continue;
            }
        };

        let out_path = cli
            .out_dir
            .join(format!("{}.{}.parquet", sanitize(&ns), sanitize(&tag)));
        if cli.skip_existing && out_path.exists() {
            eprintln!(
                "→ {ns}.{tag}  ({} cols)  SKIP (already exists: {})",
                cols.len(),
                out_path.display()
            );
            continue;
        }

        let schema = TableSchema {
            namespace: ns.clone(),
            name: tag.clone(),
            columns: cols.clone(),
        };
        eprint!("→ {ns}.{tag}  ({} cols)  ", cols.len());

        // Write to <path>.tmp, then atomic rename on success — matches the
        // pattern libpgdump uses in custom::write_archive.
        let tmp = out_path.with_extension("parquet.tmp");
        let mut sink = factory
            .open(&tmp, &schema)
            .map_err(|e| anyhow::anyhow!("opening sink for {ns}.{tag}: {e}"))?;
        let rows = drive_table(&mut reader, dump_id, sink.as_mut())?;
        let stats = sink
            .close()
            .map_err(|e| anyhow::anyhow!("closing sink for {ns}.{tag}: {e}"))?;
        std::fs::rename(&tmp, &out_path)?;
        debug_assert_eq!(rows, stats.rows_written);
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

fn make_factory(engine: Engine, opts: SinkOpts) -> anyhow::Result<Box<dyn ParquetSinkFactory>> {
    match engine {
        Engine::Rust => Ok(Box::new(pgdump2parquet_arrow::ArrowFactory::new(opts))),
        Engine::Duckdb => {
            #[cfg(feature = "duckdb")]
            {
                Ok(Box::new(pgdump2parquet_duckdb::DuckDbFactory::new(opts)))
            }
            #[cfg(not(feature = "duckdb"))]
            {
                let _ = opts;
                anyhow::bail!(
                    "the `--engine duckdb` backend requires building with the `duckdb` feature: \
                     cargo install --path crates/pgdump2parquet --features duckdb"
                )
            }
        }
    }
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

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
