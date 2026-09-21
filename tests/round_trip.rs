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

#[test]
fn test_cycle_members_sort_after_their_schema() {
    // A declared cycle between two tables must not push them ahead of the
    // schema they live in, nor ahead of the archive prelude.  See issue #14.
    let mut dump = libpgdump::new("demo", "UTF8", "18.0").expect("failed to create dump");
    let schema = dump
        .add_entry(
            libpgdump::ObjectType::Schema,
            Some(""),
            Some("app"),
            Some("postgres"),
            Some("CREATE SCHEMA app;\n"),
            None,
            None,
            &[],
        )
        .expect("failed to add schema");
    let a = dump
        .add_entry(
            libpgdump::ObjectType::Table,
            Some("app"),
            Some("a"),
            Some("postgres"),
            Some("CREATE TABLE app.a (id int);\n"),
            None,
            None,
            &[schema],
        )
        .expect("failed to add table a");
    let b = dump
        .add_entry(
            libpgdump::ObjectType::Table,
            Some("app"),
            Some("b"),
            Some("postgres"),
            Some("CREATE TABLE app.b (id int);\n"),
            None,
            None,
            &[schema, a],
        )
        .expect("failed to add table b");
    // close the loop
    dump.get_entry_mut(a)
        .expect("table a is missing")
        .dependencies
        .push(b);

    dump.sort_entries();

    let order: Vec<String> = dump
        .entries()
        .iter()
        .map(|e| format!("{} {}", e.desc.as_str(), e.tag.clone().unwrap_or_default()))
        .collect();
    let pos = |needle: &str| {
        order
            .iter()
            .position(|s| s == needle)
            .unwrap_or_else(|| panic!("{needle} missing from {order:?}"))
    };
    assert!(pos("SCHEMA app") < pos("TABLE a"), "got {order:?}");
    assert!(pos("SCHEMA app") < pos("TABLE b"), "got {order:?}");
    assert!(pos("ENCODING ") < pos("SCHEMA app"), "got {order:?}");
}

/// The TOC entries of `dump`, as `"DESC tag"` strings.
fn toc_order(dump: &libpgdump::Dump) -> Vec<String> {
    dump.entries()
        .iter()
        .map(|e| format!("{} {}", e.desc.as_str(), e.tag.clone().unwrap_or_default()))
        .collect()
}

#[test]
fn test_load_save_preserves_toc_order() {
    let Some(path) = fixture_path("pg18.custom") else {
        return;
    };
    let dump = libpgdump::load(&path).expect("failed to load fixture");
    assert!(
        !dump.sorts_on_save(),
        "a freshly loaded archive should keep its TOC order"
    );
    let before = toc_order(&dump);

    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save dump");
    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload dump");

    assert_eq!(before, toc_order(&reloaded));
}

#[test]
fn test_adding_an_entry_restores_sort_on_save() {
    let Some(path) = fixture_path("pg18.custom") else {
        return;
    };
    let mut dump = libpgdump::load(&path).expect("failed to load fixture");
    assert!(!dump.sorts_on_save());

    dump.add_entry(
        ObjectType::Schema,
        Some(""),
        Some("zzz"),
        Some("postgres"),
        Some("CREATE SCHEMA zzz;\n"),
        None,
        None,
        &[],
    )
    .expect("failed to add schema");
    assert!(
        dump.sorts_on_save(),
        "adding an entry must re-enable sorting"
    );

    let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
    dump.save(tmp.path()).expect("failed to save dump");
    let reloaded = libpgdump::load(tmp.path()).expect("failed to reload dump");

    // The appended schema sorts up into the schema section, not last.
    let order = toc_order(&reloaded);
    let schema_pos = order
        .iter()
        .position(|s| s == "SCHEMA zzz")
        .expect("missing");
    assert!(schema_pos < order.len() - 1, "got {order:?}");
}

#[test]
fn test_sort_matches_pg_dump_prelude_and_comment_placement() {
    let Some(path) = fixture_path("pg18.custom") else {
        return;
    };
    let mut dump = libpgdump::load(&path).expect("failed to load fixture");
    let pg_dump_order = toc_order(&dump);
    dump.sort_entries();
    let order = toc_order(&dump);

    // pg_dump writes ENCODING, STDSTRINGS, SEARCHPATH in that order.
    assert_eq!(&order[..3], &pg_dump_order[..3]);
    assert_eq!(order[0], "ENCODING ENCODING");
    assert_eq!(order[1], "STDSTRINGS STDSTRINGS");
    assert_eq!(order[2], "SEARCHPATH SEARCHPATH");

    // A COMMENT sorts directly after the object it describes.
    let ext = order
        .iter()
        .position(|s| s == "EXTENSION btree_gist")
        .expect("missing extension");
    assert_eq!(
        order[ext + 1],
        "COMMENT EXTENSION btree_gist",
        "got {order:?}"
    );
}
