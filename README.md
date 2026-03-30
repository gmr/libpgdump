# libpgdump

A Rust library for reading and writing PostgreSQL dump files.

Currently supports the **custom format** (`pg_dump -Fc`), with planned
support for tar and directory formats.

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
libpgdump = "0.1"
```

### Load an existing dump

```rust
use libpgdump;

let dump = libpgdump::load("backup.dump").unwrap();

println!("Database: {}", dump.dbname());
println!("Server:   {}", dump.server_version());
println!("Entries:  {}", dump.entries().len());

// Iterate over table data
for row in dump.table_data("public", "users").unwrap() {
    println!("{row}");  // tab-separated COPY format
}

// Look up a specific entry
if let Some(entry) = dump.lookup_entry("TABLE", "public", "users") {
    println!("Definition: {}", entry.defn.as_deref().unwrap_or(""));
}
```

### Create a new dump

```rust
use libpgdump;

let mut dump = libpgdump::new("mydb", "UTF8", "17.0").unwrap();

// Add a table definition
let table_id = dump.add_entry(
    "TABLE",
    Some("public"), Some("users"), Some("postgres"),
    Some("CREATE TABLE public.users (\n    id integer NOT NULL,\n    name text\n);\n"),
    Some("DROP TABLE public.users;\n"),
    None, &[],
).unwrap();

// Add table data
let data_id = dump.add_entry(
    "TABLE DATA",
    Some("public"), Some("users"), Some("postgres"),
    None, None,
    Some("COPY public.users (id, name) FROM stdin;\n"),
    &[table_id],
).unwrap();

dump.set_entry_data(data_id, b"1\tAlice\n2\tBob\n".to_vec()).unwrap();

// Save with gzip compression
dump.set_compression(libpgdump::CompressionAlgorithm::Gzip);
dump.save("output.dump").unwrap();
```

### Inspect entries

```rust
use libpgdump;

let dump = libpgdump::load("backup.dump").unwrap();

for entry in dump.entries() {
    println!(
        "{:>4} {:20} {:10} {}",
        entry.dump_id,
        entry.desc,
        entry.namespace.as_deref().unwrap_or(""),
        entry.tag.as_deref().unwrap_or(""),
    );
}
```

### Error handling

```rust
use libpgdump::{self, Error};

match libpgdump::load("backup.dump") {
    Ok(dump) => println!("Loaded {} entries", dump.entries().len()),
    Err(Error::InvalidHeader(msg)) => eprintln!("Not a valid dump: {msg}"),
    Err(Error::UnsupportedVersion(v)) => eprintln!("Unsupported version: {v}"),
    Err(e) => eprintln!("Error: {e}"),
}
```

## Supported features

- **Read/write** custom format (`-Fc`) archives
- **Compression**: none and gzip (lz4 and zstd planned)
- **Archive versions** 1.12.0 through 1.16.0 (PostgreSQL 9.0–18)
- **Version-aware parsing**: handles all format variations across versions
- **60+ object types**: tables, indexes, views, functions, constraints, extensions, etc.
- **Large object (blob) support**
- **Programmatic dump creation**: build dumps from scratch with the builder API

## Archive format reference

The custom format is a binary archive containing:

| Component | Description |
| --------- | ----------- |
| Header | Magic bytes (`PGDMP`), version, sizes, format, compression |
| Metadata | Timestamp, database name, server version, pg_dump version |
| TOC | Table of contents with entry definitions and dependencies |
| Data blocks | Compressed or uncompressed table data and large objects |

## Minimum Rust version

Rust 1.88 or later (edition 2024).

## License

BSD-3-Clause
