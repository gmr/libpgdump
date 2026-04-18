//! `pgdump2parquet` — convert a PostgreSQL dump (pg_dump `-Fc` or `-Fd`)
//! directly to Parquet, one file per table, without going through Postgres.
//!
//! This crate is a thin CLI dispatcher. The actual work lives in:
//!
//! * `pgdump2parquet-core`  — COPY/DDL parsers, sink trait, per-table driver,
//!   and both input paths (custom format via libpgdump's `CustomReader`,
//!   directory format via `DirectoryInput`).
//! * `pgdump2parquet-arrow` — arrow-rs + parquet-crate sink (all-strings).
//! * `pgdump2parquet-duckdb` — embedded DuckDB sink (typed columns).
//!
//! The `--engine` flag picks which sink factory to instantiate. The input
//! format (`-Fc` file vs `-Fd` directory) is auto-detected from the path.

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
use pgdump2parquet_core::{DirectoryInput, ddl, drive_stream, drive_table};

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

/// Unified input-source abstraction. Shared by main (for TOC inspection)
/// and the worker loop (for streaming per-table data). Cheap to clone — the
/// variants hold `Arc`s so every worker shares the parsed TOC without
/// reopening it.
#[derive(Clone)]
enum Input {
    /// `-Fc` single-file archive. Each worker opens its own File/CustomReader
    /// to get isolated seek state.
    Custom { path: Arc<PathBuf> },
    /// `-Fd` directory archive. The parsed TOC + compression are shared; each
    /// worker opens a fresh file per table on demand.
    Directory { input: Arc<DirectoryInput> },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Auto-detect format from the path. Directory = `-Fd`, file = `-Fc`.
    let input = if cli.dump.is_dir() {
        let dir_input = DirectoryInput::open(&cli.dump)
            .map_err(|e| anyhow::anyhow!("opening -Fd dump at {}: {e}", cli.dump.display()))?;
        Input::Directory {
            input: Arc::new(dir_input),
        }
    } else {
        Input::Custom {
            path: Arc::new(cli.dump.clone()),
        }
    };

    // Parse the TOC and derive (schemas, jobs) once up front. Each worker
    // operates off the shared output; the reader opened here is dropped
    // before workers start, so FDs are freed.
    let (schemas, jobs) = match &input {
        Input::Custom { path } => inspect_custom(path, &cli)?,
        Input::Directory { input } => inspect_directory(input, &cli),
    };
    let mut jobs = jobs;

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

    let nthreads = cli.parallel.max(1);
    let format_name = match &input {
        Input::Custom { .. } => "custom (-Fc)",
        Input::Directory { .. } => "directory (-Fd)",
    };
    eprintln!(
        "processing {} table(s) from {} dump across {} worker(s)",
        queue.lock().unwrap().len(),
        format_name,
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
        let input = input.clone();
        let skip_existing = cli.skip_existing;

        handles.push(thread::spawn(move || {
            if let Err(e) = worker_loop(
                worker_id,
                &input,
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
    input: &Input,
    out_dir: &Path,
    skip_existing: bool,
    factory: Arc<dyn ParquetSinkFactory>,
    queue: Arc<Mutex<Vec<Job>>>,
    schemas: Arc<HashMap<(String, String), Vec<ddl::ColumnDef>>>,
    total_tables: Arc<AtomicU64>,
    total_rows: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    // For `-Fc`, each worker owns its own `CustomReader` for seek-state
    // isolation. For `-Fd`, all workers share the same `DirectoryInput`
    // (only the TOC, which is read-only) and open fresh per-table files
    // from disk for each job.
    let mut custom_reader = match input {
        Input::Custom { path } => {
            let file = File::open(path.as_path()).map_err(|e| {
                anyhow::anyhow!("worker {worker_id}: opening {}: {e}", path.display())
            })?;
            Some(CustomReader::open(BufReader::new(file))?)
        }
        Input::Directory { .. } => None,
    };

    loop {
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

        let schema = TableSchema::new(job.namespace.clone(), job.tag.clone(), cols);
        let tmp = out_path.with_extension("parquet.tmp");
        let mut sink = factory
            .open(&tmp, &schema)
            .map_err(|e| anyhow::anyhow!("opening sink for {}.{}: {e}", job.namespace, job.tag))?;

        let rows = match (input, custom_reader.as_mut()) {
            (Input::Custom { .. }, Some(reader)) => drive_table(
                reader,
                job.dump_id,
                &schema.arrow_schema,
                sink.as_mut(),
            )?,
            (Input::Directory { input: dir }, _) => {
                match dir.open_entry_stream(job.dump_id).map_err(|e| {
                    anyhow::anyhow!("opening -Fd stream for {}.{}: {e}", job.namespace, job.tag)
                })? {
                    Some(stream) => drive_stream(stream, &schema.arrow_schema, sink.as_mut())?,
                    None => 0,
                }
            }
            _ => unreachable!("input / reader combination is invalid"),
        };

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

/// Parse a `-Fc` dump's TOC and build the schema + job list. Size hints are
/// derived from gaps between consecutive data-block offsets; that's a rough
/// proxy for on-disk compressed size, accurate enough for largest-first
/// scheduling.
fn inspect_custom(
    path: &Path,
    cli: &Cli,
) -> anyhow::Result<(
    HashMap<(String, String), Vec<ddl::ColumnDef>>,
    Vec<Job>,
)> {
    let file = File::open(path)
        .map_err(|e| anyhow::anyhow!("opening {}: {e}", path.display()))?;
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

    // Size estimate: sort data-bearing entries by offset, each entry's size
    // is the gap to the next one (or to EOF for the last).
    let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let mut offsets: Vec<(i32, u64)> = reader
        .entries()
        .iter()
        .filter(|e| e.data_state == OffsetState::Set && e.had_dumper)
        .map(|e| (e.dump_id, e.offset))
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

    let jobs: Vec<Job> = reader
        .entries()
        .iter()
        .filter(|e| e.desc == ObjectType::TableData && e.had_dumper)
        .map(|e| Job {
            dump_id: e.dump_id,
            namespace: e.namespace.clone().unwrap_or_default(),
            tag: e.tag.clone().unwrap_or_default(),
            size_hint: size_by_dump_id.get(&e.dump_id).copied().unwrap_or(0),
        })
        .filter(|j| matches_any_table(cli, j))
        .collect();

    Ok((schemas, jobs))
}

/// Parse a `-Fd` dump's `toc.dat` + each table file's on-disk size.
fn inspect_directory(
    input: &Arc<DirectoryInput>,
    cli: &Cli,
) -> (
    HashMap<(String, String), Vec<ddl::ColumnDef>>,
    Vec<Job>,
) {
    let mut schemas: HashMap<(String, String), Vec<ddl::ColumnDef>> = HashMap::new();
    for e in input.entries() {
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

    let jobs: Vec<Job> = input
        .entries()
        .iter()
        .filter(|e| e.desc == ObjectType::TableData && e.had_dumper)
        .map(|e| Job {
            dump_id: e.dump_id,
            namespace: e.namespace.clone().unwrap_or_default(),
            tag: e.tag.clone().unwrap_or_default(),
            size_hint: input.data_file_size(e.dump_id),
        })
        .filter(|j| matches_any_table(cli, j))
        .collect();

    (schemas, jobs)
}

fn matches_any_table(cli: &Cli, j: &Job) -> bool {
    cli.tables.is_empty()
        || cli.tables.iter().any(|pat| {
            matches_simple_glob(pat, &format!("{}.{}", j.namespace, j.tag))
        })
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
