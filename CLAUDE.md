# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

libpgdump is a Rust library for reading and writing PostgreSQL dump files. It currently supports the custom binary format (`-Fc`), with planned support for tar and directory formats.

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

- `src/io/primitives.rs` — Low-level read/write for pg_dump's custom integer, string, and offset encodings
- `src/format/custom.rs` — Custom format (Fc) reader and writer
- `src/compress/` — Compression layer (none, gzip; lz4/zstd stubs)
- `src/dump.rs` — Main `Dump` struct with public read/write API
- `src/entry.rs` — TOC entry model
- `src/header.rs` — Archive header model
- `src/types.rs` — Core enums (Section, Format, CompressionAlgorithm, etc.)
- `src/constants.rs` — Object type constants and section mapping
- `src/version.rs` — Archive version handling and PG version mapping

## Testing

- Unit tests are inline in each module
- Integration tests in `tests/read_dump.rs` and `tests/round_trip.rs`
- Fixture-based tests require dump files in `build/data/` (generated via `just bootstrap`)
- Tests gracefully skip when fixtures are not present

## Key Design Decisions

- Integer encoding: 1 sign byte + N magnitude bytes (LSB first). NOT standard little-endian.
- Strings: length-prefixed (pg_dump int) + UTF-8 bytes. Length -1 = NULL.
- Version-aware parsing: fields present/absent based on archive version (1.12.0–1.16.0).
- Section mapping derived from entry description, not stored separately.
