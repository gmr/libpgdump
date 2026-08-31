//! Tests that archives written by libpgdump are readable by pg_restore.
//!
//! These tests need the `pg_restore` binary on PATH. The ones driven by
//! `build/data` also need the fixtures created by `just bootstrap`; they skip
//! when either is missing.

mod common;
use common::{data_path, fixture_path};

use std::path::{Path, PathBuf};
use std::process::Command;

use libpgdump::{CompressionAlgorithm, Format};

/// Run pg_restore with the given arguments, or `None` if pg_restore is missing.
fn pg_restore(args: &[&str]) -> Option<std::process::Output> {
    match Command::new("pg_restore").args(args).output() {
        Ok(output) => Some(output),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => panic!("failed to run pg_restore: {err}"),
    }
}

/// Assert that pg_restore can read the whole archive, TOC and data alike.
///
/// `pg_restore -l` only parses the TOC, so this converts to SQL instead to
/// force every data member to be read.
fn assert_readable(path: &Path) {
    let path = path.to_string_lossy().to_string();
    let Some(output) = pg_restore(&["-f", "-", &path]) else {
        eprintln!("skipping: pg_restore not found");
        return;
    };
    assert!(
        output.status.success(),
        "pg_restore failed for {path}: status {}, stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Load `source`, save it in `format` and `compression`, and check pg_restore
/// can read the result.
fn round_trip_through_pg_restore(
    source: &Path,
    format: Format,
    compression: CompressionAlgorithm,
    out_name: &str,
) {
    let mut dump = libpgdump::load(source).expect("failed to load dump");
    dump.set_format(format);
    dump.set_compression(compression);

    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let out_path = tmp.path().join(out_name);
    dump.save(&out_path).expect("failed to save dump");

    assert_readable(&out_path);
}

/// Resolve a generated fixture, or `None` when `just bootstrap` has not run.
fn bootstrap_fixture(name: &str) -> Option<PathBuf> {
    let path = fixture_path(name);
    if path.is_none() {
        eprintln!("skipping: fixture {name} not found, run `just bootstrap`");
    }
    path
}

#[test]
fn test_pg_restore_reads_custom() {
    let Some(path) = bootstrap_fixture("dump.not-compressed") else {
        return;
    };
    round_trip_through_pg_restore(
        &path,
        Format::Custom,
        CompressionAlgorithm::None,
        "out.dump",
    );
}

/// Regression test: the directory writer used to store a NULL filename for
/// entries without a data file, which crashes pg_restore's `_ReadExtraToc`.
#[test]
fn test_pg_restore_reads_directory() {
    let Some(path) = bootstrap_fixture("dump.directory") else {
        return;
    };
    round_trip_through_pg_restore(
        &path,
        Format::Directory,
        CompressionAlgorithm::None,
        "out.dir",
    );
}

/// Regression test: tar members used to be written in filename order, which
/// pg_restore rejects when it does not match the TOC order.
#[test]
fn test_pg_restore_reads_tar() {
    let Some(path) = bootstrap_fixture("dump.directory") else {
        return;
    };
    round_trip_through_pg_restore(&path, Format::Tar, CompressionAlgorithm::None, "out.tar");
}

#[test]
fn test_pg_restore_reads_compressed_custom() {
    let Some(path) = bootstrap_fixture("dump.not-compressed") else {
        return;
    };
    for compression in [
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Zstd,
    ] {
        round_trip_through_pg_restore(&path, Format::Custom, compression, "out.dump");
    }
}

#[test]
fn test_pg_restore_reads_compressed_directory() {
    let Some(path) = bootstrap_fixture("dump.directory") else {
        return;
    };
    for compression in [
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Zstd,
    ] {
        round_trip_through_pg_restore(&path, Format::Directory, compression, "out.dir");
    }
}

/// Regression test: reading an empty string as NULL produced archives that
/// crash pg_restore before PostgreSQL 12. Modern pg_restore tolerates it, so
/// this only proves the rewritten archives stay readable; the fidelity itself
/// is checked in `legacy_archives.rs`.
#[test]
fn test_pg_restore_reads_legacy_archives() {
    for name in ["pg11-archive-1.13.dump", "pg13-archive-1.14.dump"] {
        for (format, out_name) in [
            (Format::Custom, "out.dump"),
            (Format::Directory, "out.dir"),
            (Format::Tar, "out.tar"),
        ] {
            round_trip_through_pg_restore(
                &data_path(name),
                format,
                CompressionAlgorithm::None,
                out_name,
            );
        }
    }
}
