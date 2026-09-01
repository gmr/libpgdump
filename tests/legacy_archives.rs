//! Tests for archive versions older than the CI PostgreSQL matrix.
//!
//! The archives in `tests/data` are checked in because no server in the test
//! matrix writes these versions any more.

mod common;
use common::data_path;

use libpgdump::{CompressionAlgorithm, Format};

const ARCHIVE_1_13: &str = "pg11-archive-1.13.dump";
const ARCHIVE_1_14: &str = "pg13-archive-1.14.dump";

#[test]
fn test_load_archive_1_13() {
    let dump = libpgdump::load(data_path(ARCHIVE_1_13)).expect("failed to load 1.13 archive");

    assert_eq!(dump.version().to_string(), "1.13.0");
    assert!(dump.server_version().starts_with("11."));

    let rows: Vec<&str> = dump
        .table_data("public", "t")
        .expect("failed to read table data")
        .collect();
    assert_eq!(rows.len(), 100);
}

#[test]
fn test_load_archive_1_14() {
    let dump = libpgdump::load(data_path(ARCHIVE_1_14)).expect("failed to load 1.14 archive");

    assert_eq!(dump.version().to_string(), "1.14.0");
    assert!(dump.server_version().starts_with("13."));

    let rows: Vec<&str> = dump
        .table_data("public", "t")
        .expect("failed to read table data")
        .collect();
    assert_eq!(rows.len(), 100);
}

/// Regression test: pg_dump writes empty strings for fields such as an entry's
/// owner, and pg_restore before PostgreSQL 12 calls `strlen()` on them without
/// a NULL check. Reading an empty string as NULL therefore produced archives
/// that crashed those versions, so the two cases must stay distinct.
#[test]
fn test_empty_strings_are_not_read_as_null() {
    let dump = libpgdump::load(data_path(ARCHIVE_1_13)).expect("failed to load 1.13 archive");

    let entry = dump
        .entries()
        .iter()
        .find(|e| e.tag.as_deref() == Some("ENCODING"))
        .expect("ENCODING entry not found");

    assert_eq!(entry.owner.as_deref(), Some(""));
    assert_eq!(entry.drop_stmt.as_deref(), Some(""));
}

#[test]
fn test_empty_strings_survive_round_trip() {
    for format in [Format::Custom, Format::Directory, Format::Tar] {
        let mut dump = libpgdump::load(data_path(ARCHIVE_1_13)).expect("failed to load archive");
        dump.set_format(format);

        let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
        let out_path = tmp.path().join("out");
        dump.save(&out_path).expect("failed to save dump");

        let reloaded = libpgdump::load(&out_path).expect("failed to reload dump");
        let entry = reloaded
            .entries()
            .iter()
            .find(|e| e.tag.as_deref() == Some("ENCODING"))
            .expect("ENCODING entry not found");

        assert_eq!(
            entry.owner.as_deref(),
            Some(""),
            "owner lost its empty string in {format:?} format"
        );
        assert_eq!(
            entry.drop_stmt.as_deref(),
            Some(""),
            "drop_stmt lost its empty string in {format:?} format"
        );
    }
}

/// Regression test: archive versions before 1.15 have no compression-algorithm
/// byte, so writing lz4 or zstd used to fail with `UnsupportedCompression`.
/// Selecting one now raises the version to 1.15, which carries no other change.
#[test]
fn test_modern_compression_raises_archive_version() {
    for (algorithm, expected) in [
        (CompressionAlgorithm::None, "1.14.0"),
        (CompressionAlgorithm::Gzip, "1.14.0"),
        (CompressionAlgorithm::Lz4, "1.15.0"),
        (CompressionAlgorithm::Zstd, "1.15.0"),
    ] {
        for format in [Format::Custom, Format::Directory] {
            let mut dump =
                libpgdump::load(data_path(ARCHIVE_1_14)).expect("failed to load 1.14 archive");
            dump.set_format(format);
            dump.set_compression(algorithm);
            assert_eq!(
                dump.version().to_string(),
                expected,
                "wrong archive version for {algorithm:?}"
            );

            let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
            let out_path = tmp.path().join("out");
            dump.save(&out_path)
                .unwrap_or_else(|e| panic!("failed to save {algorithm:?} {format:?}: {e}"));

            let reloaded = libpgdump::load(&out_path).expect("failed to reload dump");
            assert_eq!(reloaded.compression(), algorithm);
            assert_eq!(reloaded.version().to_string(), expected);

            let rows: Vec<&str> = reloaded
                .table_data("public", "t")
                .expect("failed to read table data")
                .collect();
            assert_eq!(rows.len(), 100, "data lost for {algorithm:?} {format:?}");
        }
    }
}
