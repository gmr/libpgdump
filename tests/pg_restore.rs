//! Tests that archives written by libpgdump are readable by pg_restore.
//!
//! These tests need the `pg_restore` binary on PATH and the fixtures created by
//! `just bootstrap`; they skip when either is missing.

mod common;
use common::fixture_path;

use std::path::Path;
use std::process::Command;

use libpgdump::Format;

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

/// Load a fixture, save it in `format`, and check pg_restore can read it back.
fn round_trip_through_pg_restore(fixture: &str, format: Format, out_name: &str) {
    let Some(path) = fixture_path(fixture) else {
        eprintln!("skipping: fixture {fixture} not found, run `just bootstrap`");
        return;
    };

    let mut dump = libpgdump::load(&path).expect("failed to load dump");
    dump.set_format(format);

    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    let out_path = tmp.path().join(out_name);
    dump.save(&out_path).expect("failed to save dump");

    assert_readable(&out_path);
}

#[test]
fn test_pg_restore_reads_custom() {
    round_trip_through_pg_restore("dump.not-compressed", Format::Custom, "out.dump");
}

/// Regression test: the directory writer used to store a NULL filename for
/// entries without a data file, which crashes pg_restore's `_ReadExtraToc`.
#[test]
fn test_pg_restore_reads_directory() {
    round_trip_through_pg_restore("dump.directory", Format::Directory, "out.dir");
}

/// Regression test: tar members used to be written in filename order, which
/// pg_restore rejects when it does not match the TOC order.
#[test]
fn test_pg_restore_reads_tar() {
    round_trip_through_pg_restore("dump.directory", Format::Tar, "out.tar");
}
