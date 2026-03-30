use std::path::Path;

fn fixture_path(name: &str) -> Option<std::path::PathBuf> {
    let path = Path::new("build/data").join(name);
    if path.exists() { Some(path) } else { None }
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

    // Compare entries
    for (orig, reload) in dump.entries().iter().zip(reloaded.entries().iter()) {
        assert_eq!(orig.dump_id, reload.dump_id, "dump_id mismatch");
        assert_eq!(orig.desc, reload.desc, "desc mismatch for dump_id {}", orig.dump_id);
        assert_eq!(orig.tag, reload.tag, "tag mismatch for dump_id {}", orig.dump_id);
        assert_eq!(
            orig.namespace, reload.namespace,
            "namespace mismatch for dump_id {}",
            orig.dump_id
        );
        assert_eq!(orig.defn, reload.defn, "defn mismatch for dump_id {}", orig.dump_id);
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
            "TABLE",
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
            "TABLE DATA",
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
            "TABLE DATA",
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
            "TABLE DATA",
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
            "TABLE DATA",
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
