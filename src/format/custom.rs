use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};

use crate::dump::Dump;
use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::io::entry_data::{
    EntryReader, read_blob_data, read_block_header, read_compressed_data, write_blob_block,
    write_data_block,
};
use crate::io::primitives::write_offset;
use crate::io::toc::{read_toc, write_toc};
use crate::toc::TableOfContents;
use crate::types::{BlockType, OffsetState};

pub use crate::types::{Blob, Timestamp};

/// The data content of a TOC entry, read on demand from a [`CustomReader`].
#[derive(Debug)]
pub enum EntryData {
    /// Raw (decompressed) bytes for a TABLE DATA entry.
    Data(Vec<u8>),
    /// List of large objects for a BLOBS entry.
    Blobs(Vec<Blob>),
}

/// Read a custom format [Dump] from a reader.
pub fn read_dump<R: Read + Seek>(r: &mut R) -> Result<Dump> {
    let toc = read_toc(r)?;

    // Read data blocks by seeking to each entry's offset
    let (data, blobs) = read_data_blocks(r, &toc, &toc.entries)?;

    Ok(Dump { toc, data, blobs })
}

/// A lazy reader for custom format (`-Fc`) PostgreSQL dumps.
///
/// Parses the header and TOC entries on construction, but defers reading
/// data blocks until explicitly requested. This allows working with
/// dumps too large to fit in memory.
///
/// # Example
///
/// ```no_run
/// use std::fs::File;
/// use std::io::BufReader;
/// use libpgdump::CustomReader;
///
/// let file = File::open("dump.sql").unwrap();
/// let mut reader = CustomReader::open(BufReader::new(file)).unwrap();
///
/// // Inspect TOC without loading data
/// for entry in reader.entries() {
///     println!("{}: {:?}", entry.dump_id, entry.desc);
/// }
///
/// // Read a specific entry's data on demand
/// if let Some(data) = reader.read_entry_data(1).unwrap() {
///     // process data
/// }
/// ```
pub struct CustomReader<R: Read + Seek> {
    reader: R,
    toc: TableOfContents,
}

impl<R: Read + Seek> CustomReader<R> {
    /// Open a custom format archive, reading only the header and TOC.
    ///
    /// No data blocks are read until explicitly requested via
    /// [`read_entry_data`](Self::read_entry_data) or
    /// [`read_entry_reader`](Self::read_entry_reader).
    pub fn open(mut reader: R) -> Result<Self> {
        let toc = read_toc(&mut reader)?;
        Ok(Self { reader, toc })
    }

    /// The archive header.
    pub fn header(&self) -> &TableOfContents {
        &self.toc
    }

    /// The archive creation timestamp.
    pub fn timestamp(&self) -> &Timestamp {
        &self.toc.timestamp
    }

    /// The database name.
    pub fn dbname(&self) -> &str {
        &self.toc.dbname
    }

    /// The PostgreSQL server version string.
    pub fn server_version(&self) -> &str {
        &self.toc.server_version
    }

    /// The pg_dump version string.
    pub fn dump_version(&self) -> &str {
        &self.toc.dump_version
    }

    /// All TOC entries.
    pub fn entries(&self) -> &[Entry] {
        &self.toc.entries
    }

    /// Read and decompress an entry's data block into memory.
    ///
    /// Returns `Ok(None)` if the entry has no data (either `data_state` is not
    /// `Set` or `had_dumper` is false). Returns an error if `dump_id` is not
    /// found in the TOC.
    pub fn read_entry_data(&mut self, dump_id: i32) -> Result<Option<EntryData>> {
        let block_type = match self.seek_to_data_block(dump_id)? {
            Some(bt) => bt,
            None => return Ok(None),
        };

        match block_type {
            BlockType::Blobs => {
                let blobs = read_blob_data(&mut self.reader, &self.toc)?;
                Ok(Some(EntryData::Blobs(blobs)))
            }
            BlockType::Data => {
                let data = read_compressed_data(&mut self.reader, &self.toc)?;
                Ok(Some(EntryData::Data(data)))
            }
        }
    }

    /// Return a streaming [`EntryReader`] for an entry's data.
    ///
    /// The returned reader implements [`Read`] and streams data
    /// one chunk at a time, keeping memory usage proportional to a single
    /// chunk rather than the entire entry.
    ///
    /// If the entry is compressed, the reader will automatically decompress on the fly.
    ///
    /// Returns `Ok(None)` if the entry has no data.
    ///
    /// Returns an error for `BLOBS` entries — use [`read_entry_data`](Self::read_entry_data)
    /// instead, because blobs have internal OID framing that doesn't map
    /// to a flat byte stream.
    pub fn read_entry_reader(&mut self, dump_id: i32) -> Result<Option<EntryReader<'_, R>>> {
        let block_type = match self.seek_to_data_block(dump_id)? {
            Some(bt) => bt,
            None => return Ok(None),
        };

        if block_type == BlockType::Blobs {
            return Err(Error::StreamingNotSupported("BLOBS".to_string()));
        }
        Ok(Some(EntryReader::new(
            &mut self.reader,
            self.toc.int_size,
            self.toc.compression,
        )?))
    }

    /// Seek to an entry's data block and return the block type.
    ///
    /// Returns `Ok(None)` if the entry has no data. Validates the block header
    /// (type byte and dump_id) after seeking.
    fn seek_to_data_block(&mut self, dump_id: i32) -> Result<Option<BlockType>> {
        let entry = self
            .toc
            .entries
            .iter()
            .find(|e| e.dump_id == dump_id)
            .ok_or(Error::InvalidDumpId(dump_id))?;

        if entry.data_state != OffsetState::Set || !entry.had_dumper {
            return Ok(None);
        }

        self.reader.seek(SeekFrom::Start(entry.offset))?;
        read_block_header(&mut self.reader, self.toc.int_size, dump_id).map(Some)
    }

    /// Read all data blocks eagerly and convert to a full [`Dump`].
    ///
    /// This is equivalent to [`Dump::load`](crate::Dump::load) but allows
    /// inspecting the TOC first before deciding to load everything.
    pub fn into_dump(mut self) -> Result<crate::dump::Dump> {
        let (data, blobs) = read_data_blocks(&mut self.reader, &self.toc, &self.toc.entries)?;
        Ok(crate::dump::Dump {
            toc: self.toc,
            data,
            blobs,
        })
    }
}

/// Read all data blocks from the archive, decompressing them.
///
/// Returns (table_data, blobs) where table_data maps dump_id to decompressed
/// bytes and blobs maps dump_id to a list of individual large objects.
#[allow(clippy::type_complexity)]
fn read_data_blocks<R: Read + Seek>(
    r: &mut R,
    toc: &TableOfContents,
    entries: &[Entry],
) -> Result<(HashMap<i32, Vec<u8>>, HashMap<i32, Vec<Blob>>)> {
    let mut data_map: HashMap<i32, Vec<u8>> = HashMap::new();
    let mut blob_map: HashMap<i32, Vec<Blob>> = HashMap::new();

    for entry in entries {
        if entry.data_state != OffsetState::Set || !entry.had_dumper {
            continue;
        }

        r.seek(SeekFrom::Start(entry.offset))?;
        let block_type = read_block_header(r, toc.int_size, entry.dump_id)?;

        match block_type {
            BlockType::Blobs => {
                blob_map.insert(entry.dump_id, read_blob_data(r, toc)?);
            }
            BlockType::Data => {
                data_map.insert(entry.dump_id, read_compressed_data(r, toc)?);
            }
        }
    }

    Ok((data_map, blob_map))
}

/// Write a [Dump] to a writer in custom format (as in using `-Fc` with pg_dump).
pub fn write_dump<W: std::io::Write + Seek>(w: &mut W, dump: &Dump) -> Result<()> {
    let offset_positions = write_toc(w, &dump.toc)?;

    // Write compression for pre-1.15 (handled inside write_header)

    // Write data blocks and record actual offsets
    let mut actual_offsets: HashMap<i32, u64> = HashMap::new();
    for entry in &dump.toc.entries {
        // Check for blob data first, then regular data
        if let Some(blobs) = dump.blobs.get(&entry.dump_id) {
            let pos = w.stream_position()?;
            actual_offsets.insert(entry.dump_id, pos);
            write_blob_block(w, &dump.toc, entry.dump_id, blobs)?;
        } else if let Some(data) = dump.data.get(&entry.dump_id) {
            let pos = w.stream_position()?;
            actual_offsets.insert(entry.dump_id, pos);
            write_data_block(w, &dump.toc, entry.dump_id, data)?;
        }
    }

    // Second pass: go back and fix the offsets
    for (i, entry) in dump.toc.entries.iter().enumerate() {
        let offset_file_pos = offset_positions[i];
        if let Some(&actual_offset) = actual_offsets.get(&entry.dump_id) {
            w.seek(SeekFrom::Start(offset_file_pos))?;
            write_offset(w, OffsetState::Set, actual_offset, dump.toc.off_size)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::{ArchiveVersion, CompressionAlgorithm, Format, ObjectType, Section};

    use super::*;

    fn make_test_toc() -> TableOfContents {
        TableOfContents {
            version: ArchiveVersion::new(1, 14, 0),
            int_size: 4,
            off_size: 8,
            format: Format::Custom,
            compression: CompressionAlgorithm::None,
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
            entries: Vec::new(),
        }
    }

    fn make_test_timestamp() -> Timestamp {
        Timestamp {
            second: 30,
            minute: 15,
            hour: 10,
            day: 25,
            month: 3,
            year: 2025,
            is_dst: 0,
        }
    }

    #[test]
    fn test_full_archive_round_trip_no_data() {
        let dump = Dump {
            toc: TableOfContents {
                entries: vec![Entry {
                    dump_id: 1,
                    had_dumper: false,
                    table_oid: "0".to_string(),
                    oid: "0".to_string(),
                    tag: Some("ENCODING".to_string()),
                    desc: ObjectType::Encoding,
                    section: Section::PreData,
                    defn: Some("SET client_encoding = 'UTF8';\n".to_string()),
                    drop_stmt: None,
                    copy_stmt: None,
                    namespace: None,
                    tablespace: None,
                    tableam: None,
                    relkind: None,
                    owner: None,
                    with_oids: false,
                    dependencies: vec![],
                    data_state: OffsetState::NoData,
                    offset: 0,
                    filename: None,
                }],
                ..make_test_toc()
            },
            data: HashMap::new(),
            blobs: HashMap::new(),
        };

        let mut buf = Cursor::new(Vec::new());
        write_dump(&mut buf, &dump).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let parsed = read_dump(&mut buf).unwrap();

        assert_eq!(parsed.toc.dbname, "testdb");
        assert_eq!(parsed.toc.server_version, "17.0");
        assert_eq!(parsed.toc.dump_version, "pg_dump (PostgreSQL) 17.0");
        assert_eq!(parsed.toc.entries.len(), 1);
        assert_eq!(parsed.toc.entries[0].desc, ObjectType::Encoding);
        assert_eq!(
            parsed.toc.entries[0].defn.as_deref(),
            Some("SET client_encoding = 'UTF8';\n")
        );
    }

    #[test]
    fn test_custom_reader_open() {
        // Build an archive with one no-data entry and one data entry
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";
        let mut dump = Dump {
            toc: TableOfContents {
                entries: vec![
                    Entry {
                        dump_id: 1,
                        had_dumper: false,
                        table_oid: "0".to_string(),
                        oid: "0".to_string(),
                        tag: Some("ENCODING".to_string()),
                        desc: ObjectType::Encoding,
                        section: Section::PreData,
                        defn: Some("SET client_encoding = 'UTF8';\n".to_string()),
                        drop_stmt: None,
                        copy_stmt: None,
                        namespace: None,
                        tablespace: None,
                        tableam: None,
                        relkind: None,
                        owner: None,
                        with_oids: false,
                        dependencies: vec![],
                        data_state: OffsetState::NoData,
                        offset: 0,
                        filename: None,
                    },
                    Entry {
                        dump_id: 2,
                        had_dumper: true,
                        table_oid: "16384".to_string(),
                        oid: "0".to_string(),
                        tag: Some("users".to_string()),
                        desc: ObjectType::TableData,
                        section: Section::Data,
                        defn: None,
                        drop_stmt: None,
                        copy_stmt: Some(
                            "COPY public.users (id, name, age) FROM stdin;\n".to_string(),
                        ),
                        namespace: Some("public".to_string()),
                        tablespace: None,
                        tableam: None,
                        relkind: None,
                        owner: Some("postgres".to_string()),
                        with_oids: false,
                        dependencies: vec![],
                        data_state: OffsetState::NotSet,
                        offset: 0,
                        filename: None,
                    },
                ],
                ..make_test_toc()
            },
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        dump.data.insert(2, data_content.to_vec());

        let mut buf = Cursor::new(Vec::new());
        write_dump(&mut buf, &dump).unwrap();

        // Open with CustomReader — should parse header + TOC only
        buf.seek(SeekFrom::Start(0)).unwrap();
        let reader = CustomReader::open(buf).unwrap();

        assert_eq!(reader.timestamp(), &make_test_timestamp());
        assert_eq!(reader.dbname(), "testdb");
        assert_eq!(reader.server_version(), "17.0");
        assert_eq!(reader.dump_version(), "pg_dump (PostgreSQL) 17.0");
        assert_eq!(reader.header().version, ArchiveVersion::new(1, 14, 0));
        assert_eq!(reader.entries().len(), 2);
        assert_eq!(reader.entries()[0].desc, ObjectType::Encoding);
        assert_eq!(reader.entries()[1].desc, ObjectType::TableData);
    }

    #[test]
    fn test_custom_reader_no_data_entry() {
        let dump = Dump {
            toc: TableOfContents {
                entries: vec![Entry {
                    dump_id: 1,
                    had_dumper: false,
                    table_oid: "0".to_string(),
                    oid: "0".to_string(),
                    tag: Some("ENCODING".to_string()),
                    desc: ObjectType::Encoding,
                    section: Section::PreData,
                    defn: Some("SET client_encoding = 'UTF8';\n".to_string()),
                    drop_stmt: None,
                    copy_stmt: None,
                    namespace: None,
                    tablespace: None,
                    tableam: None,
                    relkind: None,
                    owner: None,
                    with_oids: false,
                    dependencies: vec![],
                    data_state: OffsetState::NoData,
                    offset: 0,
                    filename: None,
                }],
                ..make_test_toc()
            },
            data: HashMap::new(),
            blobs: HashMap::new(),
        };

        let mut buf = Cursor::new(Vec::new());
        write_dump(&mut buf, &dump).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = CustomReader::open(buf).unwrap();

        let result = reader.read_entry_data(1).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_custom_reader_into_dump() {
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";
        let archive_bytes = make_data_archive(make_test_toc(), data_content);

        // Load via Dump (eager) for comparison
        let mut eager_cursor = Cursor::new(archive_bytes.clone());
        let eager_dump = read_dump(&mut eager_cursor).unwrap();

        // Load via CustomReader -> into_dump
        let lazy_reader = CustomReader::open(Cursor::new(archive_bytes)).unwrap();
        let lazy_dump = lazy_reader.into_dump().unwrap();

        assert!(lazy_dump.get_entry(1).is_some());
        assert!(eager_dump.get_entry(1).is_some());
        assert_eq!(
            lazy_dump.entry_data(1).unwrap(),
            eager_dump.entry_data(1).unwrap()
        );
    }

    fn make_data_archive(header: TableOfContents, data: &[u8]) -> Vec<u8> {
        let mut dump = Dump {
            toc: TableOfContents {
                entries: vec![Entry {
                    dump_id: 1,
                    had_dumper: true,
                    table_oid: "16384".to_string(),
                    oid: "0".to_string(),
                    tag: Some("users".to_string()),
                    desc: ObjectType::TableData,
                    section: Section::Data,
                    defn: None,
                    drop_stmt: None,
                    copy_stmt: Some("COPY public.users (id, name, age) FROM stdin;\n".to_string()),
                    namespace: Some("public".to_string()),
                    tablespace: None,
                    tableam: None,
                    relkind: None,
                    owner: Some("postgres".to_string()),
                    with_oids: false,
                    dependencies: vec![],
                    data_state: OffsetState::NotSet,
                    offset: 0,
                    filename: None,
                }],
                ..header
            },
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        dump.data.insert(1, data.to_vec());
        let mut buf = Cursor::new(Vec::new());
        write_dump(&mut buf, &dump).unwrap();
        buf.into_inner()
    }

    fn make_blob_archive(header: TableOfContents) -> Vec<u8> {
        let mut dump = Dump {
            toc: TableOfContents {
                entries: vec![Entry {
                    dump_id: 1,
                    had_dumper: true,
                    table_oid: "0".to_string(),
                    oid: "0".to_string(),
                    tag: None,
                    desc: ObjectType::Blobs,
                    section: Section::Data,
                    defn: None,
                    drop_stmt: None,
                    copy_stmt: None,
                    namespace: None,
                    tablespace: None,
                    tableam: None,
                    relkind: None,
                    owner: None,
                    with_oids: false,
                    dependencies: vec![],
                    data_state: OffsetState::NotSet,
                    offset: 0,
                    filename: None,
                }],
                ..header
            },
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        dump.blobs.insert(
            1,
            vec![
                Blob {
                    oid: 100,
                    data: b"blob-content-A".to_vec(),
                },
                Blob {
                    oid: 200,
                    data: b"blob-content-B".to_vec(),
                },
            ],
        );
        let mut buf = Cursor::new(Vec::new());
        write_dump(&mut buf, &dump).unwrap();
        buf.into_inner()
    }

    #[test]
    fn test_custom_reader_read_entry_reader_blobs_error() {
        let bytes = make_blob_archive(make_test_toc());

        let mut reader = CustomReader::open(Cursor::new(bytes)).unwrap();
        let err = reader.read_entry_reader(1).unwrap_err();
        assert!(
            matches!(err, Error::StreamingNotSupported(_)),
            "expected StreamingNotSupported, got {err:?}"
        );
    }

    #[test]
    fn test_custom_reader_invalid_dump_id() {
        let bytes = make_data_archive(make_test_toc(), b"data");

        let mut reader = CustomReader::open(Cursor::new(bytes)).unwrap();
        let err = reader.read_entry_data(999).unwrap_err();
        assert!(
            matches!(err, Error::InvalidDumpId(999)),
            "expected InvalidDumpId(999), got {err:?}"
        );
        let err = reader.read_entry_reader(999).unwrap_err();
        assert!(
            matches!(err, Error::InvalidDumpId(999)),
            "expected InvalidDumpId(999), got {err:?}"
        );
    }

    #[test]
    fn test_full_archive_round_trip_with_data() {
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";
        let bytes = make_data_archive(make_test_toc(), data_content);

        let mut cursor = Cursor::new(bytes);
        let parsed = read_dump(&mut cursor).unwrap();

        assert_eq!(parsed.toc.entries.len(), 1);
        assert_eq!(parsed.toc.entries[0].data_state, OffsetState::Set);
        assert_eq!(parsed.data.get(&1).unwrap(), data_content);
    }
}
