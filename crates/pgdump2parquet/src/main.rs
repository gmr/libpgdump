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
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::thread;

use clap::Parser;
use libpgdump::format::custom::CustomReader;
use libpgdump::types::{ObjectType, OffsetState};
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
    /// Number of tables to convert in parallel. Defaults to the number of
    /// logical CPUs. Each worker opens its own file descriptor and its own
    /// `CustomReader`, so seek state is isolated.
    #[arg(short = 'j', long, default_value_t = default_parallel())]
    parallel: usize,
    /// `PRAGMA threads` for each embedded DuckDB (only `--engine duckdb`).
    /// Default 1 — each sink is single-threaded and the CLI fans out tables
    /// across workers. Letting DuckDB also go wide causes N×M thread
    /// stampede on heavy loads. Set 0 to restore the DuckDB default
    /// (= system cores).
    #[arg(long, default_value_t = 1)]
    duckdb_threads: usize,
    /// `PRAGMA memory_limit` for each embedded DuckDB, e.g. `2GB`. With N
    /// concurrent sinks the DuckDB default (80% of RAM *per sink*) is
    /// catastrophic — leave this at its default (2GB) unless you know
    /// what you're doing.
    #[arg(long, default_value = "2GB")]
    duckdb_memory_limit: String,
}

fn default_parallel() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Parse the TOC once up front so we can plan jobs and compute size
    // estimates. The per-worker CustomReaders open fresh file descriptors
    // later; this reader is dropped when we leave the block.
    let (schemas, all_entries_for_sizing) = {
        let file = File::open(&cli.dump)
            .map_err(|e| anyhow::anyhow!("opening {}: {e}", cli.dump.display()))?;
        let reader = CustomReader::open(BufReader::new(file))?;
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
        let entries: Vec<_> = reader
            .entries()
            .iter()
            .map(|e| {
                (
                    e.dump_id,
                    e.data_state,
                    e.had_dumper,
                    e.desc.clone(),
                    e.offset,
                    e.namespace.clone().unwrap_or_default(),
                    e.tag.clone().unwrap_or_default(),
                )
            })
            .collect();
        (schemas, entries)
    };

    // Collect TABLE DATA jobs with a rough size estimate (derived from the
    // gaps between consecutive data-block offsets in the archive). The
    // estimate doesn't need to be exact — it's only used to schedule
    // largest-first so big tables start on the first workers and smaller
    // ones fill gaps as workers free up.
    let file_size = std::fs::metadata(&cli.dump).map(|m| m.len()).unwrap_or(0);
    let mut offsets: Vec<(i32, u64)> = all_entries_for_sizing
        .iter()
        .filter(|(_, state, had_dumper, _, _, _, _)| {
            *state == OffsetState::Set && *had_dumper
        })
        .map(|(id, _, _, _, off, _, _)| (*id, *off))
        .collect();
    offsets.sort_by_key(|(_, o)| *o);
    let mut size_by_dump_id: HashMap<i32, u64> = HashMap::new();
    for i in 0..offsets.len() {
        let (id, off) = offsets[i];
        let end = offsets
            .get(i + 1)
            .map(|(_, o)| *o)
            .unwrap_or(file_size.max(off));
        size_by_dump_id.insert(id, end.saturating_sub(off));
    }

    let mut jobs: Vec<Job> = all_entries_for_sizing
        .iter()
        .filter(|(_, _, had_dumper, desc, _, _, _)| {
            *desc == ObjectType::TableData && *had_dumper
        })
        .map(|(id, _, _, _, _, ns, tag)| Job {
            dump_id: *id,
            namespace: ns.clone(),
            tag: tag.clone(),
            size_hint: size_by_dump_id.get(id).copied().unwrap_or(0),
        })
        .filter(|j| {
            cli.tables.is_empty()
                || cli
                    .tables
                    .iter()
                    .any(|pat| matches_simple_glob(pat, &format!("{}.{}", j.namespace, j.tag)))
        })
        .collect();

    if cli.list {
        jobs.sort_by(|a, b| b.size_hint.cmp(&a.size_hint));
        println!("{} table(s) with data (largest first):", jobs.len());
        for j in &jobs {
            let ncols = schemas
                .get(&(j.namespace.clone(), j.tag.clone()))
                .map(|v| v.len())
                .unwrap_or(0);
            println!(
                "  #{:>4} {}.{} ({} cols, ~{} MB)",
                j.dump_id,
                j.namespace,
                j.tag,
                ncols,
                j.size_hint / 1_048_576
            );
        }
        return Ok(());
    }

    create_dir_all(&cli.out_dir)?;

    let opts = SinkOpts {
        batch_rows: cli.batch_rows,
        row_group_rows: cli.row_group_rows,
        zstd_level: cli.zstd_level,
    };
    // Shared across workers. `ParquetSinkFactory: Send + Sync` is the contract.
    let factory: Arc<dyn ParquetSinkFactory> = make_factory(
        cli.engine.clone(),
        opts,
        cli.duckdb_threads,
        &cli.duckdb_memory_limit,
    )?
    .into();

    // Sort largest first, then pop from the end so each worker grabs the
    // biggest remaining table. With N workers and one giant table, the giant
    // one starts immediately on worker 0 while the others drain the tail.
    jobs.sort_by(|a, b| a.size_hint.cmp(&b.size_hint));
    let queue: Arc<Mutex<Vec<Job>>> = Arc::new(Mutex::new(jobs));
    let schemas: Arc<HashMap<(String, String), Vec<ddl::ColumnDef>>> = Arc::new(schemas);
    let total_tables = Arc::new(AtomicU64::new(0));
    let total_rows = Arc::new(AtomicU64::new(0));
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let out_dir = Arc::new(cli.out_dir.clone());
    let dump_path = Arc::new(cli.dump.clone());

    let nthreads = cli.parallel.max(1);
    eprintln!(
        "processing {} table(s) across {} worker(s)",
        queue.lock().unwrap().len(),
        nthreads
    );

    let mut handles = Vec::with_capacity(nthreads);
    for worker_id in 0..nthreads {
        let factory = Arc::clone(&factory);
        let queue = Arc::clone(&queue);
        let schemas = Arc::clone(&schemas);
        let total_tables = Arc::clone(&total_tables);
        let total_rows = Arc::clone(&total_rows);
        let errors = Arc::clone(&errors);
        let out_dir = Arc::clone(&out_dir);
        let dump_path = Arc::clone(&dump_path);
        let skip_existing = cli.skip_existing;

        handles.push(thread::spawn(move || {
            if let Err(e) = worker_loop(
                worker_id,
                &dump_path,
                &out_dir,
                skip_existing,
                factory,
                queue,
                schemas,
                total_tables,
                total_rows,
            ) {
                errors.lock().unwrap().push(format!("worker {worker_id}: {e}"));
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }

    let errs = errors.lock().unwrap();
    if !errs.is_empty() {
        for e in errs.iter() {
            eprintln!("ERROR: {e}");
        }
        anyhow::bail!("{} worker(s) reported errors", errs.len());
    }

    eprintln!(
        "\nconverted {} table(s), {} total rows",
        total_tables.load(Ordering::Relaxed),
        total_rows.load(Ordering::Relaxed),
    );
    Ok(())
}

struct Job {
    dump_id: i32,
    namespace: String,
    tag: String,
    size_hint: u64,
}

#[allow(clippy::too_many_arguments)]
fn worker_loop(
    worker_id: usize,
    dump_path: &Path,
    out_dir: &Path,
    skip_existing: bool,
    factory: Arc<dyn ParquetSinkFactory>,
    queue: Arc<Mutex<Vec<Job>>>,
    schemas: Arc<HashMap<(String, String), Vec<ddl::ColumnDef>>>,
    total_tables: Arc<AtomicU64>,
    total_rows: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    // Each worker has its own file descriptor + CustomReader — the dump's
    // TOC is parsed once per worker, which is a few ms even for huge dumps.
    let file = File::open(dump_path)
        .map_err(|e| anyhow::anyhow!("worker {worker_id}: opening {}: {e}", dump_path.display()))?;
    let mut reader = CustomReader::open(BufReader::new(file))?;

    loop {
        // Pop the largest remaining job from the end of the queue.
        let Some(job) = queue.lock().unwrap().pop() else {
            return Ok(());
        };

        let cols = match schemas.get(&(job.namespace.clone(), job.tag.clone())) {
            Some(c) => c.clone(),
            None => {
                eprintln!(
                    "[w{worker_id}] warn: no CREATE TABLE for {}.{} (dump_id={}); skipping",
                    job.namespace, job.tag, job.dump_id,
                );
                continue;
            }
        };

        let out_path = out_dir.join(format!(
            "{}.{}.parquet",
            sanitize(&job.namespace),
            sanitize(&job.tag)
        ));
        if skip_existing && out_path.exists() {
            eprintln!(
                "[w{worker_id}] SKIP {}.{} (exists: {})",
                job.namespace,
                job.tag,
                out_path.display()
            );
            continue;
        }

        let schema = TableSchema {
            namespace: job.namespace.clone(),
            name: job.tag.clone(),
            columns: cols,
        };
        let tmp = out_path.with_extension("parquet.tmp");
        let mut sink = factory
            .open(&tmp, &schema)
            .map_err(|e| anyhow::anyhow!("opening sink for {}.{}: {e}", job.namespace, job.tag))?;
        let rows = drive_table(&mut reader, job.dump_id, sink.as_mut())?;
        let _stats = sink
            .close()
            .map_err(|e| anyhow::anyhow!("closing sink for {}.{}: {e}", job.namespace, job.tag))?;
        std::fs::rename(&tmp, &out_path)?;

        total_tables.fetch_add(1, Ordering::Relaxed);
        total_rows.fetch_add(rows as u64, Ordering::Relaxed);
        eprintln!(
            "[w{worker_id}] {}.{} ({} cols) → {} rows → {}",
            job.namespace,
            job.tag,
            schema.columns.len(),
            rows,
            out_path.display(),
        );
    }
}

fn make_factory(
    engine: Engine,
    opts: SinkOpts,
    duckdb_threads: usize,
    duckdb_memory_limit: &str,
) -> anyhow::Result<Box<dyn ParquetSinkFactory>> {
    match engine {
        Engine::Rust => {
            let _ = (duckdb_threads, duckdb_memory_limit);
            Ok(Box::new(pgdump2parquet_arrow::ArrowFactory::new(opts)))
        }
        Engine::Duckdb => {
            #[cfg(feature = "duckdb")]
            {
                let factory = pgdump2parquet_duckdb::DuckDbFactory::new(opts)
                    .with_threads(duckdb_threads)
                    .with_memory_limit(duckdb_memory_limit);
                Ok(Box::new(factory))
            }
            #[cfg(not(feature = "duckdb"))]
            {
                let _ = (opts, duckdb_threads, duckdb_memory_limit);
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
