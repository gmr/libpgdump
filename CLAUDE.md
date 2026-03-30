# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

libpgdump is a Rust library for reading and writing PostgreSQL dump files. Supports all three pg_dump formats: custom (`-Fc`), directory (`-Fd`), and tar (`-Ft`). All four compression algorithms are supported: none, gzip, lz4, zstd (tar format does not support compression).

## Build Commands

```
cargo build
cargo test
cargo test <test_name>    # run a single test
cargo clippy              # lint
cargo fmt                 # format
cargo doc --no-deps       # build docs
just check                # fmt-check + lint + test
just bootstrap            # generate test fixtures (requires Docker)
```

## Architecture

The public API is `Dump::load(path)` / `Dump::save(path)` in `src/dump.rs`. `load()` auto-detects format from file type (directory) or magic bytes (`PGDMP` = custom, `ustar` = tar).

### Format readers/writers

Each format has its own `read_archive` / `write_archive` in `src/format/`:
- `custom.rs` — Custom format (Fc), binary stream with TOC + data blocks
- `directory.rs` — Directory format (Fd), `toc.dat` file + per-entry `.dat` files
- `tar.rs` — Tar format (Ft), standard tar archive with `toc.dat` + data files

All three share the `ArchiveData` intermediate struct (defined in `custom.rs`) that `Dump` converts to/from.

### Core modules

- `src/io/primitives.rs` — Low-level read/write for pg_dump's custom integer, string, and offset encodings
- `src/compress/` — Compression layer (none, gzip, lz4, zstd) with `decompressor`/`compressor` factory functions
- `src/entry.rs` — TOC entry model
- `src/header.rs` — Archive header model
- `src/types.rs` — Core enums (Section, Format, CompressionAlgorithm, etc.)
- `src/constants.rs` — Object type constants and section mapping
- `src/version.rs` — Archive version handling and PG version mapping
- `src/error.rs` — Error types using `thiserror`

## Testing

- Unit tests are inline in each module
- Integration tests in `tests/read_dump.rs` and `tests/round_trip.rs`; shared helpers in `tests/common/mod.rs`
- Fixture-based tests require dump files in `build/data/` (generated via `just bootstrap`)
- Tests gracefully skip when fixtures are not present

## Key Design Decisions

- Integer encoding: 1 sign byte + N magnitude bytes (LSB first). NOT standard little-endian.
- Strings: length-prefixed (pg_dump int) + UTF-8 bytes. Length -1 = NULL.
- Version-aware parsing: fields present/absent based on archive version (1.12.0–1.16.0).
- Section mapping derived from entry description, not stored separately.
- Custom format writes use atomic rename (write to `.tmp`, rename on success) to avoid partial files.
