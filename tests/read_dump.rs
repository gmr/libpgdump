mod common;
use common::fixture_path;

#[test]
fn test_load_uncompressed() {
    let Some(path) = fixture_path("dump.not-compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load uncompressed dump");

    assert!(!dump.dbname().is_empty());
    assert!(!dump.server_version().is_empty());
    assert!(!dump.dump_version().is_empty());
    assert!(!dump.entries().is_empty());

    assert_eq!(dump.compression(), libpgdump::CompressionAlgorithm::None);

    // Should have ENCODING, STDSTRINGS, SEARCHPATH entries
    let has_encoding = dump.entries().iter().any(|e| e.desc == "ENCODING");
    assert!(has_encoding, "dump should have an ENCODING entry");

    // Check that there are TABLE DATA entries
    let table_data_count = dump
        .entries()
        .iter()
        .filter(|e| e.desc == "TABLE DATA")
        .count();
    assert!(table_data_count > 0, "dump should have TABLE DATA entries");
}

#[test]
fn test_load_compressed() {
    let Some(path) = fixture_path("dump.compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load compressed dump");

    assert!(!dump.dbname().is_empty());
    assert!(!dump.entries().is_empty());
    assert_eq!(dump.compression(), libpgdump::CompressionAlgorithm::Gzip);

    // TABLE DATA entries should exist and have data
    let table_data_count = dump
        .entries()
        .iter()
        .filter(|e| e.desc == "TABLE DATA")
        .count();
    assert!(
        table_data_count > 0,
        "compressed dump should have TABLE DATA entries"
    );
}

#[test]
fn test_load_schema_only() {
    let Some(path) = fixture_path("dump.no-data") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load schema-only dump");

    assert!(!dump.entries().is_empty());

    // TABLE DATA entries should exist but have no data
    for entry in dump.entries() {
        if entry.desc == "TABLE DATA" {
            assert!(
                !entry.had_dumper,
                "schema-only dump should not have data dumpers"
            );
        }
    }
}

#[test]
fn test_load_data_only() {
    let Some(path) = fixture_path("dump.data-only") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load data-only dump");
    assert!(!dump.entries().is_empty());
}

#[test]
fn test_read_table_data_uncompressed() {
    let Some(path) = fixture_path("dump.not-compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load dump");

    // pgbench_accounts should have data from pgbench initialization
    let rows: Vec<&str> = dump
        .table_data("public", "pgbench_accounts")
        .expect("failed to get pgbench_accounts data")
        .collect();
    assert!(!rows.is_empty(), "pgbench_accounts should have rows");

    // Each row should be tab-separated
    for row in &rows {
        assert!(
            row.contains('\t'),
            "rows should be tab-separated COPY format"
        );
    }
}

#[test]
fn test_read_table_data_compressed() {
    let Some(path) = fixture_path("dump.compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load dump");

    let rows: Vec<&str> = dump
        .table_data("public", "pgbench_accounts")
        .expect("failed to get pgbench_accounts data")
        .collect();
    assert!(
        !rows.is_empty(),
        "compressed pgbench_accounts should have rows"
    );
}

#[test]
fn test_lookup_entry() {
    let Some(path) = fixture_path("dump.not-compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load dump");

    let entry = dump.lookup_entry("TABLE", "public", "pgbench_accounts");
    assert!(entry.is_some(), "should find pgbench_accounts TABLE entry");
    let entry = entry.unwrap();
    assert_eq!(entry.tag.as_deref(), Some("pgbench_accounts"));
    assert!(entry.defn.is_some(), "TABLE entry should have a definition");
}

#[test]
fn test_entity_not_found() {
    let Some(path) = fixture_path("dump.not-compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load dump");

    let result = dump.table_data("nonexistent", "table");
    assert!(result.is_err());
}

#[test]
fn test_entry_sections() {
    let Some(path) = fixture_path("dump.not-compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load dump");

    for entry in dump.entries() {
        match entry.desc.as_str() {
            "TABLE" => assert_eq!(entry.section, libpgdump::Section::PreData),
            "TABLE DATA" => assert_eq!(entry.section, libpgdump::Section::Data),
            "INDEX" => assert_eq!(entry.section, libpgdump::Section::PostData),
            "CONSTRAINT" => assert_eq!(entry.section, libpgdump::Section::PostData),
            _ => {}
        }
    }
}

#[test]
fn test_entry_dependencies() {
    let Some(path) = fixture_path("dump.not-compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load dump");

    // TABLE DATA entries typically depend on their TABLE entry
    let has_deps = dump.entries().iter().any(|e| !e.dependencies.is_empty());
    assert!(has_deps, "some entries should have dependencies");
}

#[test]
fn test_load_directory() {
    let Some(path) = fixture_path("dump.directory") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load directory dump");

    assert!(!dump.dbname().is_empty());
    assert!(!dump.server_version().is_empty());
    assert!(!dump.entries().is_empty());
    assert_eq!(dump.compression(), libpgdump::CompressionAlgorithm::None);

    let table_data_count = dump
        .entries()
        .iter()
        .filter(|e| e.desc == "TABLE DATA")
        .count();
    assert!(
        table_data_count > 0,
        "directory dump should have TABLE DATA entries"
    );
}

#[test]
fn test_load_directory_compressed() {
    let Some(path) = fixture_path("dump.directory-compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load compressed directory dump");

    assert!(!dump.dbname().is_empty());
    assert!(!dump.entries().is_empty());
    assert_eq!(dump.compression(), libpgdump::CompressionAlgorithm::Gzip);
}

#[test]
fn test_read_table_data_directory() {
    let Some(path) = fixture_path("dump.directory") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load dump");

    let rows: Vec<&str> = dump
        .table_data("public", "pgbench_accounts")
        .expect("failed to get pgbench_accounts data")
        .collect();
    assert!(!rows.is_empty(), "pgbench_accounts should have rows");
    for row in &rows {
        assert!(
            row.contains('\t'),
            "rows should be tab-separated COPY format"
        );
    }
}
