# pgdump2parquet — session handoff

State of the branch `claude/pg-dump-to-parquet-l4B7P` as of the last commit
before we break for real-world testing. Everything here is pushed to origin.

## What exists

A four-crate workspace under `crates/` — three libraries plus a thin CLI
dispatcher:

* `pgdump2parquet-core`   — COPY-text + CREATE-TABLE parsers, block pipeline,
                            sink trait, per-table driver, directory-format
                            (`-Fd`) input reader. Zero modifications to the
                            base `libpgdump` crate.
* `pgdump2parquet-arrow`  — arrow-rs + parquet crate sink (all-VARCHAR output,
                            row-group-bounded memory — buffers batches and
                            flushes whole row groups at `max_row_group_size`,
                            not true streaming).
* `pgdump2parquet-duckdb` — embedded DuckDB sink via `duckdb-rs` (typed
                            output via `TRY_CAST`, resource caps applied).
* `pgdump2parquet`        — thin CLI dispatcher. This is the binary.

Input formats supported: `-Fc` (custom) and `-Fd` (directory). `-Ft` (tar) is
**not supported** — there is a dead `Format::Tar` default arm in
`core/src/directory.rs` that is never actually handled.

## Building

```bash
# Default: arrow backend, pure-Rust gzip (miniz_oxide).
cargo build --release -p pgdump2parquet

# Recommended for real workloads:
cargo build --release -p pgdump2parquet \
    --no-default-features \
    --features duckdb,fast-gzip

# `fast-gzip` swaps in zlib-rs (pure Rust, faster than zlib-ng).
# `duckdb` adds the `--engine duckdb` path (~minutes of extra compile).
```

## Running

```bash
# Auto-detects -Fc (file) vs -Fd (directory)
./target/release/pgdump2parquet <dump-path> -o <out-dir>

# Common flags
#   -j N                        parallel workers (default: num_cpus)
#   --engine {rust,duckdb}      sink backend (default: rust)
#   --list                      list tables + sizes, don't convert
#   --skip-existing             resumable: skip tables whose parquet exists
#   -t 'public.*'               glob-filter tables (repeatable)
#   --duckdb-threads 1          PRAGMA threads per DuckDB sink
#   --duckdb-memory-limit 2GB   PRAGMA memory_limit per DuckDB sink
```

## What's next (P0 blocker for further work)

**Test against a real USAspending subset.** Every follow-up item below is
speculative until we have ground truth from the real dump shape.

Subset URL (sandboxed envs can't fetch this):
  https://files.usaspending.gov/database_download/usaspending-db-subset_20260406.zip

Smoke test plan for a new session:
  1. `curl -LO https://files.usaspending.gov/database_download/usaspending-db-subset_20260406.zip`
  2. `unzip usaspending-db-subset_20260406.zip -d /tmp/usa-subset`
  3. `./target/release/pgdump2parquet /tmp/usa-subset/<dir> --list`
     → validates TOC parse + DDL parse, zero bytes of data read
  4. `./target/release/pgdump2parquet /tmp/usa-subset/<dir> -o parquet -j 8`
     → full conversion; capture wall-clock + any warnings
  5. Spot-check a parquet file via DuckDB:
     `duckdb -c "SELECT * FROM 'parquet/rpt.subaward_search.parquet' LIMIT 5"`

Likely failure modes to watch for:
  * sqlparser-rs choking on a DDL variant (partitioned inheritance, custom
    types, FDW columns). If so: P4 below — swap to `pg_query` (libpg_query
    FFI) as a fallback parser. Today a parser failure aborts that table
    (error is not recovered per-table in a structured way); document the
    actual behavior before running at scale.
  * DuckDB backend OOM on wide tables with long text columns. If so: P3.
  * Single-table wall-clock dominated by gzip decompression. If so: P2
    gives 1.3-1.8× on that case.
  * Non-UTF-8 bytes in a text field. The Arrow sink writes strings via
    `unsafe { StringArray::new_unchecked(...) }` (arrow/src/lib.rs:131-137)
    — **there is no `from_utf8_lossy` fallback**; invalid UTF-8 will either
    propagate as a bad string or surface as a hard error at Parquet
    write time. Real fix: add a `BinaryArray` sink variant.
  * Partial output files after a worker crash. The Arrow sink opens the
    target file directly (no temp file + rename), so a crash mid-write
    leaves a truncated `.parquet` on disk. `--skip-existing` will then
    skip that table on rerun — delete partials before resuming.

## USAspending smoke-test results (2026-04-18)

Ran the 5-step plan against `usaspending-db-subset_20260406.zip` (4.6 GB
zip, `-Fd` directory dump: `toc.dat` + 73 `.dat.gz` files). Build was
`--features duckdb,fast-gzip --no-default-features`. 16 vCPU / 21 GB RAM.

* `--list`: 73 tables parsed, TOC + DDL all OK. sqlparser-rs did **not**
  choke on any real DDL — P4 can stay parked.
* Full run, arrow engine (default), `-j 8`:
  **73 tables, 96.9M rows, 3.8 GB parquet, 1m18s wall / 3m50s user.**
  Zero failures. Biggest tables: `public.financial_accounts_by_awards`
  (31M rows, 60 cols), `rpt.recipient_profile` (18M), `rpt.recipient_lookup`
  (17M), `public.financial_accounts_by_program_activity_object_class` (10M).
* Widest DDL in the set: `rpt.transaction_search_fpds` (374 cols) and
  `rpt.transaction_search_fabs` (374 cols). Both parsed + converted clean.

### Head-to-head vs. Postgres (rpt.transaction_search_fpds, 374 cols)

`pg_restore -n rpt -t transaction_search_fpds` into a local pg 16
(`/usr/lib/postgresql/16/bin`, data dir `/tmp/pgfixture`, socket
`/tmp/pgrun`, port 54320). Compared pg to parquet-via-DuckDB:

| check                                          | pg                       | parquet                   | result   |
| ---------------------------------------------- | ------------------------ | ------------------------- | -------- |
| row count                                      | 214,340                  | 214,340                   | match    |
| column count                                   | 374                      | 374                       | match    |
| column names + order (all 374)                 | —                        | —                         | match    |
| `SUM(federal_action_obligation)`               | 21,942,106,132.63        | 21,942,106,132.63         | match *  |
| `SUM(award_amount)`                            | 1,005,471,884,333.28     | 1,005,471,884,333.28      | match *  |
| `SUM(LENGTH(recipient_name))`                  | 5,361,078                | 5,361,078                 | match    |
| `COUNT(*) WHERE recipient_uei IS NULL`         | 30                       | 30                        | match    |
| `COUNT(*) WHERE veteran_owned_business = TRUE` | 12,742                   | 12,742                    | match    |

\* DuckDB `TRY_CAST AS DOUBLE` loses precision on trillion-scale sums;
use `TRY_CAST AS DECIMAL(18,2)` to match pg's `NUMERIC` exactly. Noted
below as a downstream-user gotcha, not a bug.

### One finding worth filing

Parquet **page-index** MIN/MAX for VARCHAR columns is truncated to 8 bytes
by the arrow-rs writer defaults. Example for `action_date`:

```
pyarrow row-group stats:  min='1979-04-15' max='2026-04-01'   (correct)
duckdb SELECT MIN(action_date):   '1979-04-'                    (8-byte truncated)
duckdb ORDER BY action_date LIMIT 1: '1979-04-15'               (correct)
```

Data is fine — row-group Statistics are fine — but the page index the
truncated values get stored into is what DuckDB's short-circuited
`MIN()/MAX()` reads. For a string MAX this is an unsafe upper bound
(`ZZYLVTN9` < true `ZZYLVTN9ZCQ8` lexicographically) which can mis-prune
row groups under predicate pushdown on VARCHAR range predicates. Fix:
set `WriterProperties::column_index_truncate_length(None)` (or a larger
bound) in `pgdump2parquet-arrow/src/lib.rs:40`.

## Ranked follow-ups (post-smoke-test)

| Priority | Work                                                   | Notes                                            |
| -------- | ------------------------------------------------------ | ------------------------------------------------ |
| ~~P0~~   | ~~Smoke-test on USAspending subset~~                   | Done 2026-04-18. See above.                      |
| **P0**   | Disable/extend page-index VARCHAR truncation           | See "One finding worth filing" above             |
| P1       | PostGIS → GeoParquet via `ST_GeomFromHEXEWKB`          | Filed in `FUTURE.md`. DuckDB backend only.       |
| P2       | Within-table pipelining (decompress ↔ parse)           | 1.3-1.8× on single-big-table workloads           |
| P3       | Multi-part output in DuckDB backend (`--parts-rows N`) | Makes `--engine duckdb` safe for 100GB tables    |
| P4       | ~~`pg_query` fallback DDL parser~~                     | Not needed — sqlparser-rs handled real DDL       |
| P5       | CI + README                                            | Currently zero user-facing docs                  |

`FUTURE.md` in this directory has the PostGIS design sketch and other
deferred items in more detail.

## Benchmarks on the synthetic fixture

| configuration                          | 2.7M rows, 157MB dump |
| -------------------------------------- | --------------------- |
| arrow, row-at-a-time,  j=1             | 7.15 s                |
| arrow, block,          j=1             | 4.29 s                |
| arrow, block,          j=8             | 1.04 s                |
| duckdb (row-at-a-time), j=8            | 4.21 s                |
| duckdb (block),        j=8             | 1.82 s                |
| arrow, block, fast-gzip, j=8           | 1.09 s                |

## Sandbox / environment notes (for the next session)

* `files.usaspending.gov` reachability depends on the sandbox. The previous
  environment returned `x-deny-reason: host_not_allowed`; a later review
  session reached it successfully (HTTP 200, `content-length: 4945838906`).
  Probe with `curl -sSI --max-time 20 <url>` before committing to a run, and
  if blocked, fall back to a GCE VM or local box.
* pg 16 server binaries live at `/usr/lib/postgresql/16/bin/*` (Ubuntu
  `postgresql-16` pkg); `su postgres -c ...` is how the fixture tests were
  run. Data dir `/tmp/pgfixture`, unix socket `/tmp/pgrun`, port 54320.
  Stop before leaving:
  `su postgres -c "/usr/lib/postgresql/16/bin/pg_ctl -D /tmp/pgfixture -m fast -w stop"`
* Cargo lockfile resolved `duckdb = 1.2.2`; the latest published is
  `1.10502.0` but the lockfile pinned us at 1.2.2 at first resolve.
  `cargo update -p duckdb` to pull a newer one if needed.

## One-liner to resume after a fresh clone

```bash
git checkout claude/pg-dump-to-parquet-l4B7P
cargo build --release -p pgdump2parquet --features duckdb,fast-gzip --no-default-features
# then: ./target/release/pgdump2parquet <dump> --list
```
