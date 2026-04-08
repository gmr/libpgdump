use std::collections::HashMap;
use std::io::{Cursor, Read, Seek, SeekFrom};

use crate::compress;
use crate::constants::MAGIC;
use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::header::Header;
use crate::io::primitives::{
    read_byte, read_int, read_offset, read_string, write_byte, write_int, write_offset,
    write_string,
};
use crate::types::{BlockType, CompressionAlgorithm, Format, ObjectType, OffsetState, Section};
use crate::version::{ArchiveVersion, MAX_VERSION, MIN_VERSION};

/// Timestamp fields from the archive header.
#[derive(Debug, Clone)]
pub struct Timestamp {
    pub second: i32,
    pub minute: i32,
    pub hour: i32,
    pub day: i32,
    pub month: i32,
    pub year: i32,
    pub is_dst: i32,
}

/// A single large object (blob) with its OID and decompressed content.
#[derive(Debug, Clone)]
pub struct Blob {
    pub oid: i32,
    pub data: Vec<u8>,
}

/// The data content of a TOC entry, read on demand from a [`CustomReader`].
#[derive(Debug)]
pub enum EntryData {
    /// Raw (decompressed) bytes for a TABLE DATA entry.
    Data(Vec<u8>),
    /// List of large objects for a BLOBS entry.
    Blobs(Vec<Blob>),
}

/// Read result containing all parsed archive data.
#[derive(Debug)]
pub struct ArchiveData {
    pub header: Header,
    pub timestamp: Timestamp,
    pub dbname: String,
    pub server_version: String,
    pub dump_version: String,
    pub entries: Vec<Entry>,
    /// Map of dump_id -> raw (decompressed) data bytes for TABLE DATA entries.
    pub data: HashMap<i32, Vec<u8>>,
    /// Map of dump_id -> list of blobs for BLOBS entries.
    pub blobs: HashMap<i32, Vec<Blob>>,
}

/// Read a custom format archive from a reader.
pub fn read_archive<R: Read + Seek>(r: &mut R) -> Result<ArchiveData> {
    let header = read_header(r)?;
    let int_size = header.int_size;

    // For versions < 1.15, compression was encoded as an integer after the header.
    // The header reading handles this and sets the compression field.

    let timestamp = read_timestamp(r, int_size)?;
    let dbname = read_string(r, int_size)?.unwrap_or_default();
    let server_version = read_string(r, int_size)?.unwrap_or_default();
    let dump_version = read_string(r, int_size)?.unwrap_or_default();

    let toc_count = read_int(r, int_size)?;
    if toc_count < 0 {
        return Err(Error::DataIntegrity(format!(
            "invalid TOC entry count: {toc_count}"
        )));
    }
    let mut entries = Vec::with_capacity(toc_count as usize);
    for _ in 0..toc_count {
        entries.push(read_entry(r, &header)?);
    }

    // Read data blocks by seeking to each entry's offset
    let (data, blobs) = read_data_blocks(r, &header, &entries)?;

    Ok(ArchiveData {
        header,
        timestamp,
        dbname,
        server_version,
        dump_version,
        entries,
        data,
        blobs,
    })
}

/// A lazy reader for custom format (`-Fc`) PostgreSQL dump archives.
///
/// Parses the header and TOC entries on construction, but defers reading
/// data blocks until explicitly requested. This allows working with
/// archives too large to fit in memory.
///
/// # Example
///
/// ```ignore
/// use std::fs::File;
/// use std::io::BufReader;
/// use libpgdump::format::custom::CustomReader;
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
    header: Header,
    timestamp: Timestamp,
    dbname: String,
    server_version: String,
    dump_version: String,
    entries: Vec<Entry>,
}

impl<R: Read + Seek> CustomReader<R> {
    /// Open a custom format archive, reading only the header and TOC.
    ///
    /// No data blocks are read until explicitly requested via
    /// [`read_entry_data`](Self::read_entry_data) or
    /// [`read_entry_reader`](Self::read_entry_reader).
    pub fn open(mut reader: R) -> Result<Self> {
        let header = read_header(&mut reader)?;
        let int_size = header.int_size;

        let timestamp = read_timestamp(&mut reader, int_size)?;
        let dbname = read_string(&mut reader, int_size)?.unwrap_or_default();
        let server_version = read_string(&mut reader, int_size)?.unwrap_or_default();
        let dump_version = read_string(&mut reader, int_size)?.unwrap_or_default();

        let toc_count = read_int(&mut reader, int_size)?;
        if toc_count < 0 {
            return Err(Error::DataIntegrity(format!(
                "invalid TOC entry count: {toc_count}"
            )));
        }
        let mut entries = Vec::with_capacity(toc_count as usize);
        for _ in 0..toc_count {
            entries.push(read_entry(&mut reader, &header)?);
        }

        Ok(Self {
            reader,
            header,
            timestamp,
            dbname,
            server_version,
            dump_version,
            entries,
        })
    }

    /// The archive header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The archive creation timestamp.
    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// The database name.
    pub fn dbname(&self) -> &str {
        &self.dbname
    }

    /// The PostgreSQL server version string.
    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// The pg_dump version string.
    pub fn dump_version(&self) -> &str {
        &self.dump_version
    }

    /// All TOC entries.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Read and decompress an entry's data block into memory.
    ///
    /// Returns `Ok(None)` if the entry has no data (either `data_state` is not
    /// `Set` or `had_dumper` is false). Returns an error if `dump_id` is not
    /// found in the TOC.
    pub fn read_entry_data(&mut self, dump_id: i32) -> Result<Option<EntryData>> {
        let entry = self
            .entries
            .iter()
            .find(|e| e.dump_id == dump_id)
            .ok_or(Error::InvalidDumpId(dump_id))?;

        if entry.data_state != OffsetState::Set || !entry.had_dumper {
            return Ok(None);
        }

        let offset = entry.offset;
        self.reader.seek(SeekFrom::Start(offset))?;

        // Read and validate block header
        let block_type_byte = read_byte(&mut self.reader)?;
        let block_type = BlockType::from_byte(block_type_byte).ok_or_else(|| {
            Error::DataIntegrity(format!("unknown block type: {block_type_byte}"))
        })?;
        let block_dump_id = read_int(&mut self.reader, self.header.int_size)?;
        if block_dump_id != dump_id {
            return Err(Error::DataIntegrity(format!(
                "block dump_id {block_dump_id} does not match entry dump_id {dump_id}"
            )));
        }

        match block_type {
            BlockType::Blobs => {
                let blobs = read_blob_data(&mut self.reader, &self.header)?;
                Ok(Some(EntryData::Blobs(blobs)))
            }
            BlockType::Data => {
                let data = read_compressed_data(&mut self.reader, &self.header)?;
                Ok(Some(EntryData::Data(data)))
            }
        }
    }

    /// Return a streaming reader for an entry's decompressed data.
    ///
    /// The returned [`EntryReader`] implements [`Read`] and decompresses data
    /// one chunk at a time, keeping memory usage proportional to a single
    /// chunk rather than the entire entry.
    ///
    /// Returns `Ok(None)` if the entry has no data. Returns an error for
    /// `BLOBS` entries — use [`read_entry_data`](Self::read_entry_data) instead,
    /// because blobs have internal OID framing that doesn't map to a flat byte
    /// stream.
    pub fn read_entry_reader(&mut self, dump_id: i32) -> Result<Option<EntryReader<'_, R>>> {
        let entry = self
            .entries
            .iter()
            .find(|e| e.dump_id == dump_id)
            .ok_or(Error::InvalidDumpId(dump_id))?;

        if entry.data_state != OffsetState::Set || !entry.had_dumper {
            return Ok(None);
        }

        let offset = entry.offset;
        self.reader.seek(SeekFrom::Start(offset))?;

        // Read and validate block header
        let block_type_byte = read_byte(&mut self.reader)?;
        let block_type = BlockType::from_byte(block_type_byte).ok_or_else(|| {
            Error::DataIntegrity(format!("unknown block type: {block_type_byte}"))
        })?;
        let block_dump_id = read_int(&mut self.reader, self.header.int_size)?;
        if block_dump_id != dump_id {
            return Err(Error::DataIntegrity(format!(
                "block dump_id {block_dump_id} does not match entry dump_id {dump_id}"
            )));
        }

        if block_type == BlockType::Blobs {
            return Err(Error::StreamingNotSupported("BLOBS".to_string()));
        }

        Ok(Some(EntryReader {
            reader: &mut self.reader,
            int_size: self.header.int_size,
            compression: self.header.compression,
            done: false,
            decompressed_buf: Vec::new(),
            buf_pos: 0,
        }))
    }

    /// Read all data blocks eagerly and convert to a full [`Dump`].
    ///
    /// This is equivalent to [`Dump::load`](crate::Dump::load) but allows
    /// inspecting the TOC first before deciding to load everything.
    pub fn into_dump(mut self) -> Result<crate::dump::Dump> {
        let (data, blobs) = read_data_blocks(&mut self.reader, &self.header, &self.entries)?;

        let archive = ArchiveData {
            header: self.header,
            timestamp: self.timestamp,
            dbname: self.dbname,
            server_version: self.server_version,
            dump_version: self.dump_version,
            entries: self.entries,
            data,
            blobs,
        };

        Ok(crate::dump::Dump::from_archive_data(archive))
    }
}

/// A streaming reader over a single entry's decompressed data.
///
/// Reads one compressed chunk at a time from the underlying reader,
/// decompresses it, and serves bytes from the decompressed buffer.
/// Memory usage is proportional to one chunk, not the entire entry.
///
/// Implements [`Read`] so it can be used with standard I/O adapters
/// like `BufReader` or `read_to_string`.
pub struct EntryReader<'a, R: Read + Seek> {
    reader: &'a mut R,
    int_size: u8,
    compression: CompressionAlgorithm,
    done: bool,
    decompressed_buf: Vec<u8>,
    buf_pos: usize,
}

impl<R: Read + Seek> EntryReader<'_, R> {
    /// Fill the internal buffer with the next decompressed chunk.
    /// Returns `true` if data was read, `false` if no more chunks.
    fn fill_buf(&mut self) -> std::result::Result<bool, std::io::Error> {
        if self.done {
            return Ok(false);
        }

        let chunk_size = read_int(self.reader, self.int_size).map_err(std::io::Error::other)?;

        if chunk_size == 0 {
            self.done = true;
            return Ok(false);
        }
        if chunk_size < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("negative chunk size: {chunk_size}"),
            ));
        }

        let mut chunk = vec![0u8; chunk_size as usize];
        self.reader.read_exact(&mut chunk)?;

        match self.compression {
            CompressionAlgorithm::None => {
                self.decompressed_buf = chunk;
            }
            _ => {
                let cursor = Cursor::new(&chunk);
                let mut decompressor = compress::decompressor(self.compression, cursor)
                    .map_err(std::io::Error::other)?;
                self.decompressed_buf.clear();
                decompressor.read_to_end(&mut self.decompressed_buf)?;
            }
        }
        self.buf_pos = 0;
        Ok(true)
    }
}

impl<R: Read + Seek> Read for EntryReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // If buffer is exhausted, try to read the next chunk
        if self.buf_pos >= self.decompressed_buf.len() && !self.fill_buf()? {
            return Ok(0); // EOF
        }

        let available = &self.decompressed_buf[self.buf_pos..];
        let to_copy = available.len().min(buf.len());
        buf[..to_copy].copy_from_slice(&available[..to_copy]);
        self.buf_pos += to_copy;
        Ok(to_copy)
    }
}

fn read_header<R: Read>(r: &mut R) -> Result<Header> {
    // Read and validate magic bytes
    let mut magic = [0u8; 5];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(Error::InvalidHeader(format!(
            "invalid magic bytes: expected PGDMP, got {:?}",
            String::from_utf8_lossy(&magic)
        )));
    }

    // Read version
    let major = read_byte(r)?;
    let minor = read_byte(r)?;
    let rev = if major > 1 || (major == 1 && minor > 0) {
        read_byte(r)?
    } else {
        0
    };
    let version = ArchiveVersion::new(major, minor, rev);

    if version < MIN_VERSION || version > MAX_VERSION {
        return Err(Error::UnsupportedVersion(version));
    }

    let int_size = read_byte(r)?;
    if !(1..=8).contains(&int_size) {
        return Err(Error::InvalidHeader(format!(
            "invalid integer size: {int_size} (expected 1-8)"
        )));
    }

    // Offset size was added in v1.7
    let off_size = if version >= ArchiveVersion::new(1, 7, 0) {
        let s = read_byte(r)?;
        if !(1..=8).contains(&s) {
            return Err(Error::InvalidHeader(format!(
                "invalid offset size: {s} (expected 1-8)"
            )));
        }
        s
    } else {
        int_size
    };

    let format_byte = read_byte(r)?;
    let format = Format::from_byte(format_byte).ok_or(Error::UnsupportedFormat(format_byte))?;

    // Compression handling varies by version
    let compression = if version >= ArchiveVersion::new(1, 15, 0) {
        // v1.15+: explicit compression algorithm byte in header
        let comp_byte = read_byte(r)?;
        CompressionAlgorithm::from_byte(comp_byte)
            .ok_or(Error::UnsupportedCompression(comp_byte))?
    } else {
        // Pre-1.15: compression level integer follows; 0=none, >0=gzip
        let comp_level = read_int(r, int_size)?;
        if comp_level == 0 {
            CompressionAlgorithm::None
        } else {
            CompressionAlgorithm::Gzip
        }
    };

    Ok(Header {
        version,
        int_size,
        off_size,
        format,
        compression,
    })
}

fn read_timestamp<R: Read>(r: &mut R, int_size: u8) -> Result<Timestamp> {
    Ok(Timestamp {
        second: read_int(r, int_size)?,
        minute: read_int(r, int_size)?,
        hour: read_int(r, int_size)?,
        day: read_int(r, int_size)?,
        month: read_int(r, int_size)?,
        year: read_int(r, int_size)?,
        is_dst: read_int(r, int_size)?,
    })
}

fn read_entry<R: Read>(r: &mut R, header: &Header) -> Result<Entry> {
    let int_size = header.int_size;
    let off_size = header.off_size;
    let version = header.version;

    let dump_id = read_int(r, int_size)?;
    let had_dumper = read_int(r, int_size)? != 0;
    let table_oid = read_string(r, int_size)?.unwrap_or_else(|| "0".to_string());
    let oid = read_string(r, int_size)?.unwrap_or_else(|| "0".to_string());
    let tag = read_string(r, int_size)?;
    let desc: ObjectType = read_string(r, int_size)?
        .ok_or_else(|| Error::DataIntegrity("entry has no descriptor".into()))?
        .into();

    // Section integer is in the file (v>=1.11)
    let section = if version >= ArchiveVersion::new(1, 11, 0) {
        let sec_int = read_int(r, int_size)?;
        Section::from_int(sec_int).unwrap_or(Section::None)
    } else {
        Section::None
    };

    let defn = read_string(r, int_size)?;
    let drop_stmt = read_string(r, int_size)?;

    let copy_stmt = if version >= ArchiveVersion::new(1, 3, 0) {
        read_string(r, int_size)?
    } else {
        None
    };

    let namespace = if version >= ArchiveVersion::new(1, 6, 0) {
        read_string(r, int_size)?
    } else {
        None
    };

    let tablespace = if version >= ArchiveVersion::new(1, 10, 0) {
        read_string(r, int_size)?
    } else {
        None
    };

    let tableam = if version >= ArchiveVersion::new(1, 14, 0) {
        read_string(r, int_size)?
    } else {
        None
    };

    let relkind = if version >= ArchiveVersion::new(1, 16, 0) {
        let rk = read_int(r, int_size)?;
        if rk != 0 {
            char::from_u32(rk as u32)
        } else {
            None
        }
    } else {
        None
    };

    let owner = read_string(r, int_size)?;

    let with_oids = if version >= ArchiveVersion::new(1, 9, 0) {
        let s = read_string(r, int_size)?;
        s.as_deref() == Some("true")
    } else {
        false
    };

    // Dependencies: list of string dump IDs terminated by a NULL string
    let mut dependencies = Vec::new();
    if version >= ArchiveVersion::new(1, 5, 0) {
        loop {
            let dep_str = read_string(r, int_size)?;
            match dep_str {
                Some(s) if !s.is_empty() => {
                    if let Ok(dep_id) = s.parse::<i32>() {
                        dependencies.push(dep_id);
                    }
                }
                _ => break,
            }
        }
    }

    // Custom format extra TOC data: the data offset
    let (data_state, offset) = read_offset(r, off_size)?;

    Ok(Entry {
        dump_id,
        had_dumper,
        table_oid,
        oid,
        tag,
        desc,
        section,
        defn,
        drop_stmt,
        copy_stmt,
        namespace,
        tablespace,
        tableam,
        relkind,
        owner,
        with_oids,
        dependencies,
        data_state,
        offset,
        filename: None,
    })
}

/// Read all data blocks from the archive, decompressing them.
///
/// Returns (table_data, blobs) where table_data maps dump_id to decompressed
/// bytes and blobs maps dump_id to a list of individual large objects.
#[allow(clippy::type_complexity)]
fn read_data_blocks<R: Read + Seek>(
    r: &mut R,
    header: &Header,
    entries: &[Entry],
) -> Result<(HashMap<i32, Vec<u8>>, HashMap<i32, Vec<Blob>>)> {
    let mut data_map: HashMap<i32, Vec<u8>> = HashMap::new();
    let mut blob_map: HashMap<i32, Vec<Blob>> = HashMap::new();

    for entry in entries {
        if entry.data_state != OffsetState::Set || !entry.had_dumper {
            continue;
        }

        r.seek(SeekFrom::Start(entry.offset))?;

        // Read and validate block header
        let block_type_byte = read_byte(r)?;
        let block_type = BlockType::from_byte(block_type_byte).ok_or_else(|| {
            Error::DataIntegrity(format!("unknown block type: {block_type_byte}"))
        })?;
        let block_dump_id = read_int(r, header.int_size)?;
        if block_dump_id != entry.dump_id {
            return Err(Error::DataIntegrity(format!(
                "block dump_id {block_dump_id} does not match entry dump_id {}",
                entry.dump_id
            )));
        }

        match block_type {
            BlockType::Blobs => {
                blob_map.insert(entry.dump_id, read_blob_data(r, header)?);
            }
            BlockType::Data => {
                data_map.insert(entry.dump_id, read_compressed_data(r, header)?);
            }
        }
    }

    Ok((data_map, blob_map))
}

/// Read blob data from a BLK_BLOBS block.
///
/// Structure: oid(int) compressed_chunks oid(int) compressed_chunks ... 0(int)
/// Each blob's data is preceded by its OID. A zero OID terminates the sequence.
/// Returns individual (oid, decompressed_data) pairs.
fn read_blob_data<R: Read>(r: &mut R, header: &Header) -> Result<Vec<Blob>> {
    let mut blobs = Vec::new();

    loop {
        let oid = read_int(r, header.int_size)?;
        if oid == 0 {
            break;
        }
        let data = read_compressed_data(r, header)?;
        blobs.push(Blob { oid, data });
    }

    Ok(blobs)
}

/// Read and decompress a sequence of data chunks.
///
/// Each chunk: length (int), then that many bytes of compressed data.
/// A length of 0 terminates the sequence.
fn read_compressed_data<R: Read>(r: &mut R, header: &Header) -> Result<Vec<u8>> {
    let mut compressed_buf = Vec::new();

    loop {
        let chunk_size = read_int(r, header.int_size)?;
        if chunk_size == 0 {
            break;
        }
        if chunk_size < 0 {
            return Err(Error::DataIntegrity(format!(
                "negative chunk size: {chunk_size}"
            )));
        }
        let mut chunk = vec![0u8; chunk_size as usize];
        r.read_exact(&mut chunk)?;
        compressed_buf.extend_from_slice(&chunk);
    }

    if compressed_buf.is_empty() {
        return Ok(Vec::new());
    }

    match header.compression {
        CompressionAlgorithm::None => Ok(compressed_buf),
        _ => {
            let cursor = Cursor::new(&compressed_buf);
            let mut decompressor = compress::decompressor(header.compression, cursor)?;
            let mut decompressed = Vec::new();
            decompressor.read_to_end(&mut decompressed)?;
            Ok(decompressed)
        }
    }
}

/// Write a custom format archive.
pub fn write_archive<W: std::io::Write + Seek>(w: &mut W, archive: &ArchiveData) -> Result<()> {
    let int_size = archive.header.int_size;
    let off_size = archive.header.off_size;

    write_header(w, &archive.header)?;

    // Write compression for pre-1.15 (handled inside write_header)

    write_timestamp(w, &archive.timestamp, int_size)?;
    write_string(w, Some(&archive.dbname), int_size)?;
    write_string(w, Some(&archive.server_version), int_size)?;
    write_string(w, Some(&archive.dump_version), int_size)?;

    // Write entry count
    write_int(w, archive.entries.len() as i32, int_size)?;

    // First pass: write entries with placeholder offsets, recording positions
    let mut offset_positions = Vec::new();
    for entry in &archive.entries {
        offset_positions.push(write_entry(w, entry, &archive.header)?);
    }

    // Write data blocks and record actual offsets
    let mut actual_offsets: HashMap<i32, u64> = HashMap::new();
    for entry in &archive.entries {
        // Check for blob data first, then regular data
        if let Some(blobs) = archive.blobs.get(&entry.dump_id) {
            let pos = w.stream_position()?;
            actual_offsets.insert(entry.dump_id, pos);
            write_blob_block(w, &archive.header, entry.dump_id, blobs)?;
        } else if let Some(data) = archive.data.get(&entry.dump_id) {
            let pos = w.stream_position()?;
            actual_offsets.insert(entry.dump_id, pos);
            write_data_block(w, &archive.header, entry.dump_id, data)?;
        }
    }

    // Second pass: go back and fix the offsets
    for (i, entry) in archive.entries.iter().enumerate() {
        let offset_file_pos = offset_positions[i];
        if let Some(&actual_offset) = actual_offsets.get(&entry.dump_id) {
            w.seek(SeekFrom::Start(offset_file_pos))?;
            write_offset(w, OffsetState::Set, actual_offset, off_size)?;
        }
    }

    Ok(())
}

fn write_header<W: std::io::Write>(w: &mut W, header: &Header) -> Result<()> {
    w.write_all(MAGIC)?;
    write_byte(w, header.version.major)?;
    write_byte(w, header.version.minor)?;
    write_byte(w, header.version.rev)?;
    write_byte(w, header.int_size)?;

    if header.version >= ArchiveVersion::new(1, 7, 0) {
        write_byte(w, header.off_size)?;
    }

    write_byte(w, header.format as u8)?;

    if header.version >= ArchiveVersion::new(1, 15, 0) {
        write_byte(w, header.compression as u8)?;
    } else {
        // Pre-1.15: only none and gzip are valid; write compression level
        let level = match header.compression {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Gzip => 6,
            other => {
                return Err(Error::UnsupportedCompression(other as u8));
            }
        };
        write_int(w, level, header.int_size)?;
    }

    Ok(())
}

fn write_timestamp<W: std::io::Write>(w: &mut W, ts: &Timestamp, int_size: u8) -> Result<()> {
    write_int(w, ts.second, int_size)?;
    write_int(w, ts.minute, int_size)?;
    write_int(w, ts.hour, int_size)?;
    write_int(w, ts.day, int_size)?;
    write_int(w, ts.month, int_size)?;
    write_int(w, ts.year, int_size)?;
    write_int(w, ts.is_dst, int_size)?;
    Ok(())
}

/// Write an entry, returning the file position of the offset field (for later fixup).
fn write_entry<W: std::io::Write + Seek>(w: &mut W, entry: &Entry, header: &Header) -> Result<u64> {
    let int_size = header.int_size;
    let off_size = header.off_size;
    let version = header.version;

    write_int(w, entry.dump_id, int_size)?;
    write_int(w, if entry.had_dumper { 1 } else { 0 }, int_size)?;
    write_string(w, Some(&entry.table_oid), int_size)?;
    write_string(w, Some(&entry.oid), int_size)?;
    write_string(w, entry.tag.as_deref(), int_size)?;
    write_string(w, Some(entry.desc.as_str()), int_size)?;

    if version >= ArchiveVersion::new(1, 11, 0) {
        write_int(w, entry.section.to_int(), int_size)?;
    }

    write_string(w, entry.defn.as_deref(), int_size)?;
    write_string(w, entry.drop_stmt.as_deref(), int_size)?;

    if version >= ArchiveVersion::new(1, 3, 0) {
        write_string(w, entry.copy_stmt.as_deref(), int_size)?;
    }

    if version >= ArchiveVersion::new(1, 6, 0) {
        write_string(w, entry.namespace.as_deref(), int_size)?;
    }

    if version >= ArchiveVersion::new(1, 10, 0) {
        write_string(w, entry.tablespace.as_deref(), int_size)?;
    }

    if version >= ArchiveVersion::new(1, 14, 0) {
        write_string(w, entry.tableam.as_deref(), int_size)?;
    }

    if version >= ArchiveVersion::new(1, 16, 0) {
        let rk = entry.relkind.map(|c| c as i32).unwrap_or(0);
        write_int(w, rk, int_size)?;
    }

    write_string(w, entry.owner.as_deref(), int_size)?;

    if version >= ArchiveVersion::new(1, 9, 0) {
        write_string(
            w,
            Some(if entry.with_oids { "true" } else { "false" }),
            int_size,
        )?;
    }

    if version >= ArchiveVersion::new(1, 5, 0) {
        for dep in &entry.dependencies {
            write_string(w, Some(&dep.to_string()), int_size)?;
        }
        // Terminate with NULL
        write_string(w, None, int_size)?;
    }

    // Record position of offset for later fixup
    let offset_pos = w.stream_position()?;
    write_offset(w, entry.data_state, entry.offset, off_size)?;

    Ok(offset_pos)
}

/// Write a BLK_DATA block (block type + dump_id + compressed chunks + terminator).
fn write_data_block<W: std::io::Write>(
    w: &mut W,
    header: &Header,
    dump_id: i32,
    data: &[u8],
) -> Result<()> {
    write_byte(w, BlockType::Data as u8)?;
    write_int(w, dump_id, header.int_size)?;

    if data.is_empty() {
        write_int(w, 0, header.int_size)?;
        return Ok(());
    }

    write_compressed_chunks(w, header, data)?;

    // Write terminator (zero-length chunk)
    write_int(w, 0, header.int_size)?;
    Ok(())
}

/// Write a BLK_BLOBS block from individual blob entries.
///
/// Structure: block_type + dump_id + (oid + compressed_chunks + terminator)... + oid(0)
fn write_blob_block<W: std::io::Write>(
    w: &mut W,
    header: &Header,
    dump_id: i32,
    blobs: &[Blob],
) -> Result<()> {
    write_byte(w, BlockType::Blobs as u8)?;
    write_int(w, dump_id, header.int_size)?;

    for blob in blobs {
        write_int(w, blob.oid, header.int_size)?;
        write_compressed_chunks(w, header, &blob.data)?;
        // Terminator for this blob's data
        write_int(w, 0, header.int_size)?;
    }

    // Terminating zero OID
    write_int(w, 0, header.int_size)?;
    Ok(())
}

/// Write data as compressed (or uncompressed) chunks.
fn write_compressed_chunks<W: std::io::Write>(
    w: &mut W,
    header: &Header,
    data: &[u8],
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    match header.compression {
        CompressionAlgorithm::None => {
            for chunk in data.chunks(4096) {
                write_int(w, chunk.len() as i32, header.int_size)?;
                w.write_all(chunk)?;
            }
        }
        _ => {
            let mut compressed = Vec::new();
            {
                let mut comp = compress::compressor(header.compression, &mut compressed)?;
                comp.write_all(data)?;
                comp.flush()?;
            }
            write_int(w, compressed.len() as i32, header.int_size)?;
            w.write_all(&compressed)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn make_test_header() -> Header {
        Header {
            version: ArchiveVersion::new(1, 14, 0),
            int_size: 4,
            off_size: 8,
            format: Format::Custom,
            compression: CompressionAlgorithm::None,
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
    fn test_header_round_trip() {
        let header = make_test_header();
        let mut buf = Vec::new();
        write_header(&mut buf, &header).unwrap();

        let mut cursor = Cursor::new(&buf);
        let parsed = read_header(&mut cursor).unwrap();
        assert_eq!(parsed.version, header.version);
        assert_eq!(parsed.int_size, header.int_size);
        assert_eq!(parsed.off_size, header.off_size);
        assert_eq!(parsed.format, header.format);
        assert_eq!(parsed.compression, header.compression);
    }

    #[test]
    fn test_timestamp_round_trip() {
        let ts = make_test_timestamp();
        let mut buf = Vec::new();
        write_timestamp(&mut buf, &ts, 4).unwrap();

        let mut cursor = Cursor::new(&buf);
        let parsed = read_timestamp(&mut cursor, 4).unwrap();
        assert_eq!(parsed.second, ts.second);
        assert_eq!(parsed.minute, ts.minute);
        assert_eq!(parsed.hour, ts.hour);
        assert_eq!(parsed.day, ts.day);
        assert_eq!(parsed.month, ts.month);
        assert_eq!(parsed.year, ts.year);
        assert_eq!(parsed.is_dst, ts.is_dst);
    }

    #[test]
    fn test_full_archive_round_trip_no_data() {
        let archive = ArchiveData {
            header: make_test_header(),
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
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
            data: HashMap::new(),
            blobs: HashMap::new(),
        };

        let mut buf = Cursor::new(Vec::new());
        write_archive(&mut buf, &archive).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let parsed = read_archive(&mut buf).unwrap();

        assert_eq!(parsed.dbname, "testdb");
        assert_eq!(parsed.server_version, "17.0");
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].desc, ObjectType::Encoding);
        assert_eq!(
            parsed.entries[0].defn.as_deref(),
            Some("SET client_encoding = 'UTF8';\n")
        );
    }

    #[test]
    fn test_custom_reader_open() {
        // Build an archive with one no-data entry and one data entry
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";
        let mut archive = ArchiveData {
            header: make_test_header(),
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
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
                },
            ],
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        archive.data.insert(2, data_content.to_vec());

        let mut buf = Cursor::new(Vec::new());
        write_archive(&mut buf, &archive).unwrap();

        // Open with CustomReader — should parse header + TOC only
        buf.seek(SeekFrom::Start(0)).unwrap();
        let reader = CustomReader::open(buf).unwrap();

        assert_eq!(reader.dbname(), "testdb");
        assert_eq!(reader.server_version(), "17.0");
        assert_eq!(reader.dump_version(), "pg_dump (PostgreSQL) 17.0");
        assert_eq!(reader.header().version, ArchiveVersion::new(1, 14, 0));
        assert_eq!(reader.entries().len(), 2);
        assert_eq!(reader.entries()[0].desc, ObjectType::Encoding);
        assert_eq!(reader.entries()[1].desc, ObjectType::TableData);
    }

    #[test]
    fn test_custom_reader_read_entry_data() {
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";
        let mut archive = ArchiveData {
            header: make_test_header(),
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
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
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        archive.data.insert(1, data_content.to_vec());

        let mut buf = Cursor::new(Vec::new());
        write_archive(&mut buf, &archive).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = CustomReader::open(buf).unwrap();

        let result = reader.read_entry_data(1).unwrap();
        match result {
            Some(EntryData::Data(bytes)) => assert_eq!(bytes, data_content),
            other => panic!("expected Some(EntryData::Data), got {other:?}"),
        }
    }

    #[test]
    fn test_custom_reader_no_data_entry() {
        let archive = ArchiveData {
            header: make_test_header(),
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
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
            data: HashMap::new(),
            blobs: HashMap::new(),
        };

        let mut buf = Cursor::new(Vec::new());
        write_archive(&mut buf, &archive).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = CustomReader::open(buf).unwrap();

        let result = reader.read_entry_data(1).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_custom_reader_read_entry_reader() {
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";
        let mut archive = ArchiveData {
            header: make_test_header(),
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
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
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        archive.data.insert(1, data_content.to_vec());

        let mut buf = Cursor::new(Vec::new());
        write_archive(&mut buf, &archive).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = CustomReader::open(buf).unwrap();

        // Stream the data via Read trait
        let mut entry_reader = reader.read_entry_reader(1).unwrap().unwrap();
        let mut streamed = Vec::new();
        entry_reader.read_to_end(&mut streamed).unwrap();

        assert_eq!(streamed, data_content);
    }

    #[test]
    fn test_custom_reader_into_dump() {
        use crate::Dump;

        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";
        let mut archive = ArchiveData {
            header: make_test_header(),
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
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
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        archive.data.insert(1, data_content.to_vec());

        let mut buf = Cursor::new(Vec::new());
        write_archive(&mut buf, &archive).unwrap();
        let archive_bytes = buf.into_inner();

        // Load via Dump (eager) for comparison
        let mut eager_cursor = Cursor::new(archive_bytes.clone());
        let eager_archive = read_archive(&mut eager_cursor).unwrap();
        let eager_dump = Dump::from_archive_data(eager_archive);

        // Load via CustomReader -> into_dump
        let lazy_cursor = Cursor::new(archive_bytes);
        let lazy_reader = CustomReader::open(lazy_cursor).unwrap();
        let lazy_dump = lazy_reader.into_dump().unwrap();

        assert_eq!(lazy_dump.dbname(), eager_dump.dbname());
        assert_eq!(lazy_dump.server_version(), eager_dump.server_version());
        assert_eq!(lazy_dump.entries().len(), eager_dump.entries().len());
        assert_eq!(
            lazy_dump.entry_data(1).unwrap(),
            eager_dump.entry_data(1).unwrap()
        );
    }

    #[test]
    fn test_full_archive_round_trip_with_data() {
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";

        let mut archive = ArchiveData {
            header: make_test_header(),
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
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
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        archive.data.insert(1, data_content.to_vec());

        let mut buf = Cursor::new(Vec::new());
        write_archive(&mut buf, &archive).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let parsed = read_archive(&mut buf).unwrap();

        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].data_state, OffsetState::Set);
        assert_eq!(parsed.data.get(&1).unwrap(), data_content);
    }
}
