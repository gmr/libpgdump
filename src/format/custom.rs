use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};

use crate::dump::Dump;
use crate::entry::Entry;
use crate::error::{Error, Result};
pub use crate::format::custom_entry_data::TableDataReader;
use crate::format::custom_entry_data::{
    read_blob_data, read_block_header, read_table_data, write_blob_block, write_data_block,
};
use crate::format::custom_toc::{read_toc, write_toc};
use crate::toc::TableOfContents;
use crate::types::{BlockType, OffsetState};

use crate::types::{Blob};

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

/// Read data entries from a custom format archive on demand, without loading the entire dump into memory.
///
/// Allows for streaming TABLE DATA and BLOBs entry data with a Reader interface,
/// without loading the entire dump into memory at once.
///
/// # Example
///
/// ```no_run
/// use std::fs::File;
/// use std::io::BufReader;
/// use libpgdump::CustomDataLoader;
///
/// let file = File::open("dump.sql").unwrap();
/// let mut loader = CustomDataLoader::from_reader(BufReader::new(file)).unwrap();
///
/// // Inspect TOC without loading data
/// for entry in &loader.toc.entries {
///     println!("{}: {:?}", entry.dump_id, entry.desc);
/// }
///
/// // Read a specific entry's data on demand
/// if let Some(reader) = loader.open_data_reader(1).unwrap() {
///     let mut data = Vec::new();
///     reader.read_to_end(&mut data).unwrap();
///     println!("Entry 1 data: {} bytes", data.len());
/// }
/// ```
pub struct CustomDataLoader<R: Read + Seek> {
    pub reader: R,
    pub toc: TableOfContents,
}

impl<R: Read + Seek> CustomDataLoader<R> {
    /// Open a custom format archive and read the TableOfContents.
    ///
    /// No data blocks are read until explicitly requested via
    /// [`open_entry_reader`](Self::open_entry_reader).
    pub fn from_readable(mut reader: R) -> Result<Self> {
        let toc = read_toc(&mut reader)?;
        Ok(Self { reader, toc })
    }

    /// Open a streaming [`TableDataReader`] for a particular data entry's data.
    /// 
    /// This only will error for anything besides a [ObjectType::TableData] entry.
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
    pub fn open_data_reader(&mut self, dump_id: i32) -> Result<Option<TableDataReader<'_, R>>> {
        let block_type = match self.seek_to_data_block(dump_id)? {
            Some(bt) => bt,
            None => return Ok(None),
        };

        if block_type == BlockType::Blobs {
            return Err(Error::StreamingNotSupported("BLOBS".to_string()));
        }
        Ok(Some(TableDataReader::new(
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
                data_map.insert(entry.dump_id, read_table_data(r, toc)?);
            }
        }
    }

    Ok((data_map, blob_map))
}

/// Write a [Dump] to a writer in custom format (as in using `-Fc` with pg_dump).
pub fn write_dump<W: std::io::Write>(w: &mut W, dump: &Dump) -> Result<usize> {
    let mut toc = dump.toc.clone();

    let mut toc_probe = Vec::new();
    write_toc(&mut toc_probe, &toc)?;
    let mut next_offset = toc_probe.len() as u64;

    for entry in &mut toc.entries {
        if let Some(blobs) = dump.blobs.get(&entry.dump_id) {
            let block_size =
                write_blob_block(&mut std::io::sink(), &dump.toc, entry.dump_id, blobs)?;
            entry.data_state = OffsetState::Set;
            entry.offset = next_offset;
            next_offset += block_size as u64;
        } else if let Some(data) = dump.data.get(&entry.dump_id) {
            let block_size =
                write_data_block(&mut std::io::sink(), &dump.toc, entry.dump_id, data)?;
            entry.data_state = OffsetState::Set;
            entry.offset = next_offset;
            next_offset += block_size as u64;
        }
    }

    write_toc(w, &toc)?;
    let mut written = toc_probe.len();

    for entry in &toc.entries {
        // Keep blob precedence to match existing behavior when both maps contain a dump_id.
        if let Some(blobs) = dump.blobs.get(&entry.dump_id) {
            written += write_blob_block(w, &toc, entry.dump_id, blobs)?;
        } else if let Some(data) = dump.data.get(&entry.dump_id) {
            written += write_data_block(w, &toc, entry.dump_id, data)?;
        }
    }

    Ok(written)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::{ArchiveVersion, CompressionAlgorithm, Format, ObjectType, Section, Timestamp};

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

        let mut bytes = Vec::new();
        let written = write_dump(&mut bytes, &dump).unwrap();
        assert_eq!(written, bytes.len());

        let mut reader = Cursor::new(bytes);
        let parsed = read_dump(&mut reader).unwrap();

        assert_eq!(parsed.toc, dump.toc);
        assert_eq!(parsed.data.len(), 0);
        assert_eq!(parsed.blobs.len(), 0);
        assert_eq!(parsed.toc.entries[0], dump.toc.entries[0]);
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

        let mut buf = Vec::new();
        let n_bytes_written = write_dump(&mut buf, &dump).unwrap();
        assert_eq!(n_bytes_written, buf.len());

        // Open with CustomReader — should parse header + TOC only
        let buf_cursor = Cursor::new(buf);
        let reader = CustomDataLoader::from_readable(buf_cursor).unwrap();

        // can't compare entire TOC due to offset differences, but can check key fields and entries
        assert_eq!(reader.toc.timestamp, make_test_timestamp());
        assert_eq!(reader.toc.dbname, "testdb");
        assert_eq!(reader.toc.server_version, "17.0");
        assert_eq!(reader.toc.dump_version, "pg_dump (PostgreSQL) 17.0");
        assert_eq!(reader.toc.version, ArchiveVersion::new(1, 14, 0));
        assert_eq!(reader.toc.entries.len(), 2);
        assert_eq!(reader.toc.entries[0].desc, ObjectType::Encoding);
        assert_eq!(reader.toc.entries[1].desc, ObjectType::TableData);
    }

    #[test]
    fn test_custom_reader_into_dump() {
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";
        let archive_bytes = make_data_archive(make_test_toc(), data_content);

        // Load via Dump (eager) for comparison
        let mut eager_cursor = Cursor::new(archive_bytes.clone());
        let eager_dump = read_dump(&mut eager_cursor).unwrap();

        // Load via CustomReader -> into_dump
        let lazy_reader = CustomDataLoader::from_readable(Cursor::new(archive_bytes)).unwrap();
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

    fn make_archive_with_data(header: TableOfContents) -> Vec<u8> {
        let mut dump = Dump {
            toc: TableOfContents {
                entries: vec![
                    Entry {
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
        dump.data.insert(2, b"table-data".to_vec());
        let mut buf = Vec::new();
        let n_bytes_written = write_dump(&mut buf, &dump).unwrap();
        assert_eq!(n_bytes_written, buf.len());
        buf
    }

    #[test]
    fn test_data_reader_streaming() {
        let bytes = make_archive_with_data(make_test_toc());

        let mut reader = CustomDataLoader::from_readable(Cursor::new(bytes)).unwrap();
        
        let mut data_reader = reader.open_data_reader(2).unwrap().expect("entry 2 should have data");
        let mut data_content = Vec::new();
        data_reader.read_to_end(&mut data_content).unwrap();
        assert_eq!(data_content, b"table-data".to_vec());
        drop(data_reader);

        // BLOBs are not yet supported for streaming reads, so should return an error
        let err = reader.open_data_reader(1).unwrap_err();
        assert!(
            matches!(err, Error::StreamingNotSupported(_)),
            "expected StreamingNotSupported, got {err:?}"
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
