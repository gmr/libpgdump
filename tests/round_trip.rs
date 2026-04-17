mod common;
use common::fixture_path;
use libpgdump::ObjectType;

#[test]
fn test_get_entry_mut() {
    let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("failed to create dump");

    let table_id = dump
        .add_entry(
            ObjectType::Table,
            Some("public"),
            Some("widgets"),
            Some("postgres"),
            Some("CREATE TABLE public.widgets (id int);\n"),
            None,
            None,
            &[],
        )
        .expect("failed to add entry");

    // Verify initial state
    let entry = dump.get_entry(table_id).expect("entry not found");
    assert_eq!(entry.tableam, None);
    assert_eq!(entry.owner, Some("postgres".to_string()));

    // Mutate via get_entry_mut
    let entry = dump.get_entry_mut(table_id).expect("entry not found");
    entry.tableam = Some("heap".to_string());
    entry.tablespace = Some("fast_storage".to_string());

    // Verify mutation persisted
    let entry = dump.get_entry(table_id).expect("entry not found");
    assert_eq!(entry.tableam, Some("heap".to_string()));
    assert_eq!(entry.tablespace, Some("fast_storage".to_string()));

    // Save and reload to verify fields survive round-trip
    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save dump");

    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload dump");
    let entry = reloaded.get_entry(table_id).expect("entry not found");
    assert_eq!(entry.tableam, Some("heap".to_string()));
    assert_eq!(entry.tablespace, Some("fast_storage".to_string()));
}

#[test]
fn test_get_entry_mut_not_found() {
    let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("failed to create dump");
    assert!(dump.get_entry_mut(9999).is_none());
}

#[test]
fn test_round_trip_uncompressed() {
    let Some(path) = fixture_path("dump.not-compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };

    let dump = libpgdump::load(&path).expect("failed to load dump");

    // Save to a temp file
    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save dump");

    // Reload and compare
    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload dump");

    assert_eq!(dump.dbname(), reloaded.dbname());
    assert_eq!(dump.server_version(), reloaded.server_version());
    assert_eq!(dump.dump_version(), reloaded.dump_version());
    assert_eq!(dump.version(), reloaded.version());
    assert_eq!(dump.entries().len(), reloaded.entries().len());

    // Compare entries by dump_id (order may differ due to topological sorting)
    for orig in dump.entries().iter() {
        let reload = reloaded
            .get_entry(orig.dump_id)
            .unwrap_or_else(|| panic!("missing entry with dump_id {}", orig.dump_id));
        assert_eq!(
            orig.desc, reload.desc,
            "desc mismatch for dump_id {}",
            orig.dump_id
        );
        assert_eq!(
            orig.tag, reload.tag,
            "tag mismatch for dump_id {}",
            orig.dump_id
        );
        assert_eq!(
            orig.namespace, reload.namespace,
            "namespace mismatch for dump_id {}",
            orig.dump_id
        );
        assert_eq!(
            orig.defn, reload.defn,
            "defn mismatch for dump_id {}",
            orig.dump_id
        );
        assert_eq!(
            orig.copy_stmt, reload.copy_stmt,
            "copy_stmt mismatch for dump_id {}",
            orig.dump_id
        );
    }

    // Compare table data for pgbench_accounts
    if let Ok(orig_rows) = dump.table_data("public", "pgbench_accounts") {
        let orig_rows: Vec<&str> = orig_rows.collect();
        let reload_rows: Vec<&str> = reloaded
            .table_data("public", "pgbench_accounts")
            .expect("failed to get reloaded table data")
            .collect();
        assert_eq!(
            orig_rows.len(),
            reload_rows.len(),
            "row count mismatch for pgbench_accounts"
        );
        for (i, (orig, reload)) in orig_rows.iter().zip(reload_rows.iter()).enumerate() {
            assert_eq!(orig, reload, "row {i} mismatch for pgbench_accounts");
        }
    }
}

#[test]
fn test_round_trip_new_dump() {
    let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("failed to create dump");

    // Add a table entry
    let table_id = dump
        .add_entry(
            ObjectType::Table,
            Some("public"),
            Some("users"),
            Some("postgres"),
            Some("CREATE TABLE public.users (\n    id integer NOT NULL,\n    name text\n);\n"),
            Some("DROP TABLE public.users;\n"),
            None,
            &[],
        )
        .expect("failed to add table entry");

    // Add a table data entry
    let data_id = dump
        .add_entry(
            ObjectType::TableData,
            Some("public"),
            Some("users"),
            Some("postgres"),
            None,
            None,
            Some("COPY public.users (id, name) FROM stdin;\n"),
            &[table_id],
        )
        .expect("failed to add table data entry");

    // Set the data
    let data = b"1\tAlice\n2\tBob\n3\tCharlie\n";
    dump.set_entry_data(data_id, data.to_vec())
        .expect("failed to set entry data");

    // Save and reload
    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save dump");

    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload dump");

    assert_eq!(reloaded.dbname(), "testdb");
    assert_eq!(reloaded.server_version(), "17.0");

    let rows: Vec<&str> = reloaded
        .table_data("public", "users")
        .expect("failed to get table data")
        .collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], "1\tAlice");
    assert_eq!(rows[1], "2\tBob");
    assert_eq!(rows[2], "3\tCharlie");
}

#[test]
fn test_round_trip_compressed_new_dump() {
    let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("failed to create dump");
    dump.set_compression(libpgdump::CompressionAlgorithm::Gzip);

    let data_id = dump
        .add_entry(
            ObjectType::TableData,
            Some("public"),
            Some("items"),
            Some("postgres"),
            None,
            None,
            Some("COPY public.items (id, value) FROM stdin;\n"),
            &[],
        )
        .expect("failed to add entry");

    // Generate some data
    let mut data = String::new();
    for i in 0..100 {
        data.push_str(&format!("{i}\tvalue_{i}\n"));
    }
    dump.set_entry_data(data_id, data.into_bytes())
        .expect("failed to set data");

    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save dump");

    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload dump");
    let rows: Vec<&str> = reloaded
        .table_data("public", "items")
        .expect("failed to get table data")
        .collect();
    assert_eq!(rows.len(), 100);
    assert_eq!(rows[0], "0\tvalue_0");
    assert_eq!(rows[99], "99\tvalue_99");
}

#[test]
fn test_round_trip_lz4_new_dump() {
    let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("failed to create dump");
    dump.set_compression(libpgdump::CompressionAlgorithm::Lz4);

    let data_id = dump
        .add_entry(
            ObjectType::TableData,
            Some("public"),
            Some("items"),
            Some("postgres"),
            None,
            None,
            Some("COPY public.items (id, value) FROM stdin;\n"),
            &[],
        )
        .expect("failed to add entry");

    let mut data = String::new();
    for i in 0..100 {
        data.push_str(&format!("{i}\tvalue_{i}\n"));
    }
    dump.set_entry_data(data_id, data.into_bytes())
        .expect("failed to set data");

    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save dump");

    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload dump");
    assert_eq!(reloaded.compression(), libpgdump::CompressionAlgorithm::Lz4);
    let rows: Vec<&str> = reloaded
        .table_data("public", "items")
        .expect("failed to get table data")
        .collect();
    assert_eq!(rows.len(), 100);
    assert_eq!(rows[0], "0\tvalue_0");
    assert_eq!(rows[99], "99\tvalue_99");
}

#[test]
fn test_round_trip_zstd_new_dump() {
    let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("failed to create dump");
    dump.set_compression(libpgdump::CompressionAlgorithm::Zstd);

    let data_id = dump
        .add_entry(
            ObjectType::TableData,
            Some("public"),
            Some("items"),
            Some("postgres"),
            None,
            None,
            Some("COPY public.items (id, value) FROM stdin;\n"),
            &[],
        )
        .expect("failed to add entry");

    let mut data = String::new();
    for i in 0..100 {
        data.push_str(&format!("{i}\tvalue_{i}\n"));
    }
    dump.set_entry_data(data_id, data.into_bytes())
        .expect("failed to set data");

    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save dump");

    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload dump");
    assert_eq!(
        reloaded.compression(),
        libpgdump::CompressionAlgorithm::Zstd
    );
    let rows: Vec<&str> = reloaded
        .table_data("public", "items")
        .expect("failed to get table data")
        .collect();
    assert_eq!(rows.len(), 100);
    assert_eq!(rows[0], "0\tvalue_0");
    assert_eq!(rows[99], "99\tvalue_99");
}

#[test]
fn test_round_trip_blobs() {
    let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("failed to create dump");

    let blob1_data = b"hello blob 1".to_vec();
    let blob2_data = vec![0u8, 1, 2, 3, 255, 254, 253];
    dump.add_blob(16601, blob1_data.clone())
        .expect("failed to add blob 1");
    dump.add_blob(16602, blob2_data.clone())
        .expect("failed to add blob 2");

    // Verify before save
    let blobs = dump.blobs();
    assert_eq!(blobs.len(), 2);
    assert_eq!(blobs[0].0, 16601);
    assert_eq!(blobs[0].1, b"hello blob 1");
    assert_eq!(blobs[1].0, 16602);
    assert_eq!(blobs[1].1, &[0u8, 1, 2, 3, 255, 254, 253]);

    // Save and reload
    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save dump");

    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload dump");
    let blobs = reloaded.blobs();
    assert_eq!(blobs.len(), 2);
    assert_eq!(blobs[0].0, 16601);
    assert_eq!(blobs[0].1, blob1_data.as_slice());
    assert_eq!(blobs[1].0, 16602);
    assert_eq!(blobs[1].1, blob2_data.as_slice());
}

#[test]
fn test_read_blobs_from_fixture() {
    let Some(path) = fixture_path("dump.not-compressed") else {
        eprintln!("Skipping: fixture not found. Run `just bootstrap` to generate.");
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load dump");
    let blobs = dump.blobs();
    if !blobs.is_empty() {
        for (oid, data) in &blobs {
            assert!(*oid > 0, "blob OID should be positive");
            assert!(!data.is_empty(), "blob data should not be empty");
        }
    }
}

#[test]
fn test_round_trip_directory_format() {
    let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("failed to create dump");
    dump.set_format(libpgdump::Format::Directory);

    let table_id = dump
        .add_entry(
            ObjectType::Table,
            Some("public"),
            Some("items"),
            Some("postgres"),
            Some("CREATE TABLE public.items (id int, value text);\n"),
            Some("DROP TABLE public.items;\n"),
            None,
            &[],
        )
        .expect("failed to add table entry");

    let data_id = dump
        .add_entry(
            ObjectType::TableData,
            Some("public"),
            Some("items"),
            Some("postgres"),
            None,
            None,
            Some("COPY public.items (id, value) FROM stdin;\n"),
            &[table_id],
        )
        .expect("failed to add data entry");

    let mut data = String::new();
    for i in 0..50 {
        data.push_str(&format!("{i}\tvalue_{i}\n"));
    }
    dump.set_entry_data(data_id, data.into_bytes())
        .expect("failed to set data");

    dump.add_blob(99001, b"directory blob data".to_vec())
        .expect("failed to add blob");

    let tmp = tempfile::TempDir::new().expect("failed to create temp dir");
    dump.save(tmp.path())
        .expect("failed to save directory dump");

    // Verify files
    assert!(tmp.path().join("toc.dat").exists());

    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload directory dump");
    assert_eq!(reloaded.dbname(), "testdb");

    let rows: Vec<&str> = reloaded
        .table_data("public", "items")
        .expect("failed to get table data")
        .collect();
    assert_eq!(rows.len(), 50);
    assert_eq!(rows[0], "0\tvalue_0");

    let blobs = reloaded.blobs();
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].0, 99001);
    assert_eq!(blobs[0].1, b"directory blob data");
}

#[test]
fn test_round_trip_tar_format() {
    let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("failed to create dump");
    dump.set_format(libpgdump::Format::Tar);

    let data_id = dump
        .add_entry(
            ObjectType::TableData,
            Some("public"),
            Some("items"),
            Some("postgres"),
            None,
            None,
            Some("COPY public.items (id, value) FROM stdin;\n"),
            &[],
        )
        .expect("failed to add data entry");

    dump.set_entry_data(data_id, b"1\tAlice\n2\tBob\n".to_vec())
        .expect("failed to set data");

    dump.add_blob(42, b"tar blob data".to_vec())
        .expect("failed to add blob");

    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save tar dump");

    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload tar dump");
    assert_eq!(reloaded.dbname(), "testdb");

    let rows: Vec<&str> = reloaded
        .table_data("public", "items")
        .expect("failed to get table data")
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], "1\tAlice");
    assert_eq!(rows[1], "2\tBob");

    let blobs = reloaded.blobs();
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].0, 42);
    assert_eq!(blobs[0].1, b"tar blob data");
}

/// Verify `CustomReader::read_entry_stream` works for all compression
/// algorithms (previously it bailed out on compressed archives).
#[test]
fn test_read_entry_stream_all_compressions() {
    use std::io::{BufReader, Read};

    for alg in [
        libpgdump::CompressionAlgorithm::None,
        libpgdump::CompressionAlgorithm::Gzip,
        libpgdump::CompressionAlgorithm::Lz4,
        libpgdump::CompressionAlgorithm::Zstd,
    ] {
        let mut dump = libpgdump::new("testdb", "UTF8", "17.0").expect("new");
        dump.set_compression(alg);
        let data_id = dump
            .add_entry(
                ObjectType::TableData,
                Some("public"),
                Some("items"),
                Some("postgres"),
                None,
                None,
                Some("COPY public.items (id, value) FROM stdin;\n"),
                &[],
            )
            .expect("add");
        let mut expected = String::new();
        for i in 0..500 {
            expected.push_str(&format!("{i}\tvalue_{i}\n"));
        }
        dump.set_entry_data(data_id, expected.clone().into_bytes())
            .expect("set_entry_data");

        let tmp = tempfile::NamedTempFile::new().expect("tmpfile");
        dump.save(tmp.path()).expect("save");

        let file = std::fs::File::open(tmp.path()).expect("open");
        let mut reader =
            libpgdump::format::custom::CustomReader::open(BufReader::new(file)).expect("open");
        let dump_id = reader
            .entries()
            .iter()
            .find(|e| e.desc == ObjectType::TableData && e.tag.as_deref() == Some("items"))
            .expect("entry")
            .dump_id;
        let mut stream = reader
            .read_entry_stream(dump_id)
            .expect("stream")
            .expect("has data");
        let mut got = String::new();
        stream.read_to_string(&mut got).expect("read");
        assert_eq!(got, expected, "mismatch for compression {alg:?}");
    }
}
