use crate::io::primitives::{read_byte, read_int, write_byte, write_int};
use crate::{Blob, BlockType, CompressionAlgorithm, TableOfContents, compress};
use crate::{Error, Result};
use std::io::Read;

/// Read and validate a data block header (block type byte + dump_id).
/// The reader must already be positioned at the start of the block.
pub(crate) fn read_block_header<R: Read>(
    r: &mut R,
    int_size: u8,
    expected_dump_id: i32,
) -> Result<BlockType> {
    let block_type_byte = read_byte(r)?;
    let block_type = BlockType::from_byte(block_type_byte)
        .ok_or_else(|| Error::DataIntegrity(format!("unknown block type: {block_type_byte}")))?;
    let block_dump_id = read_int(r, int_size)?;
    if block_dump_id != expected_dump_id {
        return Err(Error::DataIntegrity(format!(
            "block dump_id {block_dump_id} does not match entry dump_id {expected_dump_id}"
        )));
    }
    Ok(block_type)
}

/// Read blob data from a BLK_BLOBS block.
///
/// Structure: oid(int) compressed_chunks oid(int) compressed_chunks ... 0(int)
/// Each blob's data is preceded by its OID. A zero OID terminates the sequence.
/// Returns individual (oid, decompressed_data) pairs.
pub(crate) fn read_blob_data<R: Read>(r: &mut R, toc: &TableOfContents) -> Result<Vec<Blob>> {
    let mut blobs = Vec::new();

    loop {
        let oid = read_int(r, toc.int_size)?;
        if oid == 0 {
            break;
        }
        let data = read_compressed_data(r, toc)?;
        blobs.push(Blob { oid, data });
    }

    Ok(blobs)
}

/// Read and (if needed) decompress all of the chunks of an entry
///
/// Each chunk: length (int), then that many bytes of compressed data.
/// A length of 0 terminates the sequence.
pub(crate) fn read_compressed_data<R: Read>(r: &mut R, toc: &TableOfContents) -> Result<Vec<u8>> {
    let reader = EntryReader::new(r, toc.int_size, toc.compression)?;
    let mut decompressed_data = Vec::new();
    let mut buf_reader = std::io::BufReader::new(reader);
    buf_reader.read_to_end(&mut decompressed_data)?;
    Ok(decompressed_data)
}

/// Either a [`RawEntryReader`] for uncompressed TABLE DATA or a [`CompressedEntryReader`] for compressed data.
#[derive(Debug)]
pub enum EntryReader<'a, R: Read> {
    /// A streaming reader for uncompressed TABLE DATA.
    Raw(RawEntryReader<'a, R>),
    /// A streaming reader for compressed data
    Compressed(CompressedEntryReader<'a, R>),
}

impl<'a, R: Read> EntryReader<'a, R> {
    pub fn new(reader: &'a mut R, int_size: u8, compression: CompressionAlgorithm) -> Result<Self> {
        let raw_reader = RawEntryReader::new(reader, int_size);
        if compression == CompressionAlgorithm::None {
            return Ok(EntryReader::Raw(raw_reader));
        }

        let decompressor = compress::decompressor(compression, raw_reader)?;
        Ok(EntryReader::Compressed(CompressedEntryReader::new(
            decompressor,
        )))
    }
}

impl<R: Read> Read for EntryReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            EntryReader::Raw(reader) => reader.read(buf),
            EntryReader::Compressed(reader) => reader.read(buf),
        }
    }
}

/// A streaming reader for an entry's raw (decompressed) data.
///
/// This typically wraps a RawEntryReader, but any Read will work.
///
/// The result from read() is the next chunk of uncompressed data.
pub struct CompressedEntryReader<'a, R: Read> {
    decompressor: Box<dyn Read + 'a>,
    _marker: std::marker::PhantomData<R>,
}

impl<R: Read> std::fmt::Debug for CompressedEntryReader<'_, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressedEntryReader").finish()
    }
}

impl<'a, R: Read> CompressedEntryReader<'a, R> {
    fn new(decompressor: Box<dyn Read + 'a>) -> Self {
        Self {
            decompressor,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<R: Read> Read for CompressedEntryReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.decompressor.read(buf)
    }
}

/// A streaming reader over a single entry's raw data.
///
/// If the entry is compressed, you will need to wrap it with a decompressor.
///
/// Implements [`Read`] so it can be used with standard I/O adapters
/// like `BufReader` or `read_to_string`.
///
/// The data format is a sequence of chunks:
/// Each chunk: length (int), then that many bytes of raw (either compressed or uncompressed) data.
/// A length of 0 terminates the sequence.
pub struct RawEntryReader<'a, R: Read> {
    reader: &'a mut R,
    int_size: u8,
    done: bool,
    chunk_remaining: usize,
}

impl<R: Read> std::fmt::Debug for RawEntryReader<'_, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntryReader")
            .field("done", &self.done)
            .field("chunk_remaining", &self.chunk_remaining)
            .finish()
    }
}

impl<R: Read> RawEntryReader<'_, R> {
    fn new(reader: &mut R, int_size: u8) -> RawEntryReader<'_, R> {
        RawEntryReader {
            reader,
            int_size,
            done: false,
            chunk_remaining: 0,
        }
    }

    /// Return the remaining bytes in the current chunk.
    ///
    /// If no chunk is currently active, this reads the next chunk header so
    /// callers can size their output buffer before calling `read`.
    pub fn remaining_bytes_in_chunk(&mut self) -> std::io::Result<usize> {
        if self.chunk_remaining == 0 {
            self.fill_chunk_header()?;
        }
        Ok(self.chunk_remaining)
    }

    /// Read the next chunk size from the archive stream.
    fn fill_chunk_header(&mut self) -> std::result::Result<(), std::io::Error> {
        if self.done {
            return Ok(());
        }

        let chunk_size = read_int(self.reader, self.int_size).map_err(std::io::Error::other)?;

        if chunk_size == 0 {
            self.done = true;
            self.chunk_remaining = 0;
            return Ok(());
        }
        if chunk_size < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("negative chunk size: {chunk_size}"),
            ));
        }

        self.chunk_remaining = chunk_size as usize;
        Ok(())
    }
}

impl<R: Read> Read for RawEntryReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.chunk_remaining == 0 {
            self.fill_chunk_header()?;
        }
        if self.chunk_remaining == 0 {
            return Ok(0);
        }

        let to_copy = self.chunk_remaining.min(buf.len());
        self.reader.read_exact(&mut buf[..to_copy])?;
        self.chunk_remaining -= to_copy;
        Ok(to_copy)
    }
}

/// Write a BLK_DATA block (block type + dump_id + compressed chunks + terminator).
pub(crate) fn write_data_block<W: std::io::Write>(
    w: &mut W,
    toc: &TableOfContents,
    dump_id: i32,
    data: &[u8],
) -> Result<()> {
    write_byte(w, BlockType::Data as u8)?;
    write_int(w, dump_id, toc.int_size)?;

    if data.is_empty() {
        write_int(w, 0, toc.int_size)?;
        return Ok(());
    }

    write_compressed_chunks(w, toc, data)?;

    // Write terminator (zero-length chunk)
    write_int(w, 0, toc.int_size)?;
    Ok(())
}

/// Write a BLK_BLOBS block from individual blob entries.
///
/// Structure: block_type + dump_id + (oid + compressed_chunks + terminator)... + oid(0)
pub(crate) fn write_blob_block<W: std::io::Write>(
    w: &mut W,
    toc: &TableOfContents,
    dump_id: i32,
    blobs: &[Blob],
) -> Result<()> {
    write_byte(w, BlockType::Blobs as u8)?;
    write_int(w, dump_id, toc.int_size)?;

    for blob in blobs {
        write_int(w, blob.oid, toc.int_size)?;
        write_compressed_chunks(w, toc, &blob.data)?;
        // Terminator for this blob's data
        write_int(w, 0, toc.int_size)?;
    }

    // Terminating zero OID
    write_int(w, 0, toc.int_size)?;
    Ok(())
}

/// Write data as compressed (or uncompressed) chunks.
fn write_compressed_chunks<W: std::io::Write>(
    w: &mut W,
    toc: &TableOfContents,
    data: &[u8],
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    match toc.compression {
        CompressionAlgorithm::None => {
            for chunk in data.chunks(4096) {
                write_int(w, chunk.len() as i32, toc.int_size)?;
                w.write_all(chunk)?;
            }
        }
        _ => {
            let mut compressed = Vec::new();
            {
                let mut comp = compress::compressor(toc.compression, &mut compressed)?;
                comp.write_all(data)?;
                comp.flush()?;
            }
            write_int(w, compressed.len() as i32, toc.int_size)?;
            w.write_all(&compressed)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    use crate::{ArchiveVersion, CompressionAlgorithm, Format, TableOfContents, Timestamp};

    use super::{
        EntryReader, read_blob_data, read_block_header, read_compressed_data, write_blob_block,
        write_data_block,
    };

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

    fn make_test_toc(compression: CompressionAlgorithm) -> TableOfContents {
        TableOfContents {
            version: ArchiveVersion::new(1, 15, 0),
            int_size: 4,
            off_size: 8,
            format: Format::Custom,
            compression,
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
            entries: Vec::new(),
        }
    }

    #[test]
    fn test_data_block_round_trip_uncompressed() {
        let toc = make_test_toc(CompressionAlgorithm::None);
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";

        let mut buf = Cursor::new(Vec::new());
        write_data_block(&mut buf, &toc, 1, data_content).unwrap();

        buf.set_position(0);
        let block_type = read_block_header(&mut buf, toc.int_size, 1).unwrap();
        assert_eq!(block_type as u8, crate::BlockType::Data as u8);

        let parsed = read_compressed_data(&mut buf, &toc).unwrap();
        assert_eq!(parsed, data_content);
    }

    #[test]
    fn test_data_block_round_trip_gzip() {
        let toc = make_test_toc(CompressionAlgorithm::Gzip);
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";

        let mut buf = Cursor::new(Vec::new());
        write_data_block(&mut buf, &toc, 1, data_content).unwrap();

        buf.set_position(0);
        let block_type = read_block_header(&mut buf, toc.int_size, 1).unwrap();
        assert_eq!(block_type as u8, crate::BlockType::Data as u8);

        let parsed = read_compressed_data(&mut buf, &toc).unwrap();
        assert_eq!(parsed, data_content);
    }

    #[test]
    fn test_blob_block_round_trip_uncompressed() {
        let toc = make_test_toc(CompressionAlgorithm::None);
        let blobs = vec![
            crate::Blob {
                oid: 100,
                data: b"blob-content-A".to_vec(),
            },
            crate::Blob {
                oid: 200,
                data: b"blob-content-B".to_vec(),
            },
        ];

        let mut buf = Cursor::new(Vec::new());
        write_blob_block(&mut buf, &toc, 1, &blobs).unwrap();

        buf.set_position(0);
        let block_type = read_block_header(&mut buf, toc.int_size, 1).unwrap();
        assert_eq!(block_type as u8, crate::BlockType::Blobs as u8);

        let parsed = read_blob_data(&mut buf, &toc).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].oid, 100);
        assert_eq!(parsed[0].data, b"blob-content-A");
        assert_eq!(parsed[1].oid, 200);
        assert_eq!(parsed[1].data, b"blob-content-B");
    }

    #[test]
    fn test_blob_block_round_trip_gzip() {
        let toc = make_test_toc(CompressionAlgorithm::Gzip);
        let blobs = vec![
            crate::Blob {
                oid: 100,
                data: b"blob-content-A".to_vec(),
            },
            crate::Blob {
                oid: 200,
                data: b"blob-content-B".to_vec(),
            },
        ];

        let mut buf = Cursor::new(Vec::new());
        write_blob_block(&mut buf, &toc, 1, &blobs).unwrap();

        buf.set_position(0);
        let block_type = read_block_header(&mut buf, toc.int_size, 1).unwrap();
        assert_eq!(block_type as u8, crate::BlockType::Blobs as u8);

        let parsed = read_blob_data(&mut buf, &toc).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].oid, 100);
        assert_eq!(parsed[0].data, b"blob-content-A");
        assert_eq!(parsed[1].oid, 200);
        assert_eq!(parsed[1].data, b"blob-content-B");
    }

    #[test]
    fn test_entry_reader_streams_uncompressed_data() {
        let toc = make_test_toc(CompressionAlgorithm::None);
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";

        let mut buf = Cursor::new(Vec::new());
        write_data_block(&mut buf, &toc, 1, data_content).unwrap();

        buf.set_position(0);
        read_block_header(&mut buf, toc.int_size, 1).unwrap();

        let mut reader = EntryReader::new(&mut buf, toc.int_size, toc.compression).unwrap();
        let mut streamed = Vec::new();
        reader.read_to_end(&mut streamed).unwrap();
        assert_eq!(streamed, data_content);
    }

    #[test]
    fn test_entry_reader_streams_gzip_data() {
        let toc = make_test_toc(CompressionAlgorithm::Gzip);
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";

        let mut buf = Cursor::new(Vec::new());
        write_data_block(&mut buf, &toc, 1, data_content).unwrap();

        buf.set_position(0);
        read_block_header(&mut buf, toc.int_size, 1).unwrap();

        let mut reader = EntryReader::new(&mut buf, toc.int_size, toc.compression).unwrap();
        let mut streamed = Vec::new();
        reader.read_to_end(&mut streamed).unwrap();
        assert_eq!(streamed, data_content);
    }

    #[test]
    fn test_raw_entry_reader_remaining_bytes_in_chunk() {
        let toc = make_test_toc(CompressionAlgorithm::None);
        let data = vec![b'x'; 5000];

        let mut buf = Cursor::new(Vec::new());
        write_data_block(&mut buf, &toc, 1, &data).unwrap();

        buf.set_position(0);
        read_block_header(&mut buf, toc.int_size, 1).unwrap();

        let mut entry_reader = EntryReader::new(&mut buf, toc.int_size, toc.compression).unwrap();
        match &mut entry_reader {
            EntryReader::Raw(raw) => {
                assert_eq!(raw.remaining_bytes_in_chunk().unwrap(), 4096);

                let mut first = vec![0u8; raw.remaining_bytes_in_chunk().unwrap()];
                raw.read_exact(&mut first).unwrap();
                assert!(first.iter().all(|b| *b == b'x'));

                assert_eq!(raw.remaining_bytes_in_chunk().unwrap(), 904);

                let mut second = vec![0u8; raw.remaining_bytes_in_chunk().unwrap()];
                raw.read_exact(&mut second).unwrap();
                assert!(second.iter().all(|b| *b == b'x'));

                assert_eq!(raw.remaining_bytes_in_chunk().unwrap(), 0);
            }
            EntryReader::Compressed(_) => panic!("expected raw entry reader"),
        }
    }
}
