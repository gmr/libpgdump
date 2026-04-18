# pgdump2parquet — session handoff

State of the branch `claude/pg-dump-to-parquet-l4B7P` as of the last commit
before we break for real-world testing. Everything here is pushed to origin.

## What exists

A three-crate workspace under `crates/`:

* `pgdump2parquet-core`   — COPY-text + CREATE-TABLE parsers, block pipeline,
                            sink trait, per-table driver, directory-format
                            (`-Fd`) input reader. Zero modifications to the
                            base `libpgdump` crate.
* `pgdump2parquet-arrow`  — arrow-rs + parquet crate sink (all-VARCHAR output,
                            streaming row groups, bounded memory).
* `pgdump2parquet-duckdb` — embedded DuckDB sink via `duckdb-rs` (typed
                            output via `TRY_CAST`, resource caps applied).
* `pgdump2parquet`        — thin CLI dispatcher. This is the binary.

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
    FFI) as a fallback parser.
  * DuckDB backend OOM on wide tables with long text columns. If so: P3.
  * Single-table wall-clock dominated by gzip decompression. If so: P2
    gives 1.3-1.8× on that case.
  * Non-UTF-8 bytes in a text field triggering a `StringArray` encode
    error. Arrow sink falls back to `from_utf8_lossy` today; may need
    a `BinaryArray` sink variant instead.

## Ranked follow-ups (post-smoke-test)

| Priority | Work                                                   | Notes                                            |
| -------- | ------------------------------------------------------ | ------------------------------------------------ |
| **P0**   | Smoke-test on USAspending subset                       | Run the 5-step plan above                        |
| P1       | PostGIS → GeoParquet via `ST_GeomFromHEXEWKB`          | Filed in `FUTURE.md`. DuckDB backend only.       |
| P2       | Within-table pipelining (decompress ↔ parse)           | 1.3-1.8× on single-big-table workloads           |
| P3       | Multi-part output in DuckDB backend (`--parts-rows N`) | Makes `--engine duckdb` safe for 100GB tables    |
| P4       | `pg_query` fallback DDL parser                         | Only if sqlparser-rs chokes on real-world DDL    |
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

* `files.usaspending.gov` is NOT reachable from the coding-sandbox we were in —
  `x-deny-reason: host_not_allowed` on the proxy. Run the smoke test from a
  GCE VM or local dev box with general internet.
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
