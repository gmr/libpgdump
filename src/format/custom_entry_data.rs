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
        let data = read_table_data(r, toc)?;
        blobs.push(Blob { oid, data });
    }

    Ok(blobs)
}

/// Read and (if needed) decompress all of the chunks of an entry
///
/// Each chunk: length (int), then that many bytes of compressed data.
/// A length of 0 terminates the sequence.
pub(crate) fn read_table_data<R: Read>(r: &mut R, toc: &TableOfContents) -> Result<Vec<u8>> {
    let reader = TableDataReader::new(r, toc.int_size, toc.compression)?;
    let mut decompressed_data = Vec::new();
    let mut buf_reader = std::io::BufReader::new(reader);
    buf_reader.read_to_end(&mut decompressed_data)?;
    Ok(decompressed_data)
}

/// Either a [`RawEntryReader`] for uncompressed TABLE DATA or a [`CompressedEntryReader`] for compressed data.
#[derive(Debug)]
pub enum TableDataReader<'a, R: Read> {
    /// A streaming reader for uncompressed TABLE DATA.
    Raw(RawTableDataReader<'a, R>),
    /// A streaming reader for compressed data
    Compressed(CompressedTableDataReader<'a, R>),
}

impl<'a, R: Read> TableDataReader<'a, R> {
    pub fn new(reader: &'a mut R, int_size: u8, compression: CompressionAlgorithm) -> Result<Self> {
        let raw_reader = RawTableDataReader::new(reader, int_size);
        if compression == CompressionAlgorithm::None {
            return Ok(TableDataReader::Raw(raw_reader));
        }

        let decompressor = compress::decompressor(compression, raw_reader)?;
        Ok(TableDataReader::Compressed(CompressedTableDataReader::new(
            decompressor,
        )))
    }
}

impl<R: Read> Read for TableDataReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            TableDataReader::Raw(reader) => reader.read(buf),
            TableDataReader::Compressed(reader) => reader.read(buf),
        }
    }
}

/// A streaming reader for an entry's raw (decompressed) data.
///
/// This typically wraps a RawEntryReader, but any Read will work.
///
/// The result from read() is the next chunk of uncompressed data.
pub struct CompressedTableDataReader<'a, R: Read> {
    decompressor: Box<dyn Read + 'a>,
    _marker: std::marker::PhantomData<R>,
}

impl<R: Read> std::fmt::Debug for CompressedTableDataReader<'_, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressedTableDataReader").finish()
    }
}

impl<'a, R: Read> CompressedTableDataReader<'a, R> {
    fn new(decompressor: Box<dyn Read + 'a>) -> Self {
        Self {
            decompressor,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<R: Read> Read for CompressedTableDataReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.decompressor.read(buf)
    }
}

/// A streaming reader over a table data entry's raw data.
///
/// If the entry is compressed, you will need to wrap it with a decompressor.
///
/// Implements [`Read`] so it can be used with standard I/O adapters
/// like `BufReader` or `read_to_string`.
///
/// The data format is a sequence of chunks:
/// Each chunk: length (int), then that many bytes of raw (either compressed or uncompressed) data.
/// A length of 0 terminates the sequence.
pub struct RawTableDataReader<'a, R: Read> {
    reader: &'a mut R,
    int_size: u8,
    done: bool,
    chunk_remaining: usize,
}

impl<R: Read> std::fmt::Debug for RawTableDataReader<'_, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawTableDataReader")
            .field("done", &self.done)
            .field("chunk_remaining", &self.chunk_remaining)
            .finish()
    }
}

impl<R: Read> RawTableDataReader<'_, R> {
    fn new(reader: &mut R, int_size: u8) -> RawTableDataReader<'_, R> {
        RawTableDataReader {
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

impl<R: Read> Read for RawTableDataReader<'_, R> {
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
) -> Result<usize> {
    let mut written = 0;
    written += write_byte(w, BlockType::Data as u8)?;
    written += write_int(w, dump_id, toc.int_size)?;

    if data.is_empty() {
        written += write_int(w, 0, toc.int_size)?;
        return Ok(written);
    }

    written += write_compressed_chunks(w, toc, data)?;

    // Write terminator (zero-length chunk)
    written += write_int(w, 0, toc.int_size)?;
    Ok(written)
}

/// Write a BLK_BLOBS block from individual blob entries.
///
/// Structure: block_type + dump_id + (oid + compressed_chunks + terminator)... + oid(0)
pub(crate) fn write_blob_block<W: std::io::Write>(
    w: &mut W,
    toc: &TableOfContents,
    dump_id: i32,
    blobs: &[Blob],
) -> Result<usize> {
    let mut written = 0;
    written += write_byte(w, BlockType::Blobs as u8)?;
    written += write_int(w, dump_id, toc.int_size)?;

    for blob in blobs {
        written += write_int(w, blob.oid, toc.int_size)?;
        written += write_compressed_chunks(w, toc, &blob.data)?;
        // Terminator for this blob's data
        written += write_int(w, 0, toc.int_size)?;
    }

    // Terminating zero OID
    written += write_int(w, 0, toc.int_size)?;
    Ok(written)
}

/// Write data as compressed (or uncompressed) chunks.
fn write_compressed_chunks<W: std::io::Write>(
    w: &mut W,
    toc: &TableOfContents,
    data: &[u8],
) -> Result<usize> {
    if data.is_empty() {
        return Ok(0);
    }

    let mut written = 0;

    match toc.compression {
        CompressionAlgorithm::None => {
            for chunk in data.chunks(4096) {
                written += write_int(w, chunk.len() as i32, toc.int_size)?;
                w.write_all(chunk)?;
                written += chunk.len();
            }
        }
        _ => {
            let mut compressed = Vec::new();
            {
                let mut comp = compress::compressor(toc.compression, &mut compressed)?;
                comp.write_all(data)?;
                comp.flush()?;
            }
            written += write_int(w, compressed.len() as i32, toc.int_size)?;
            w.write_all(&compressed)?;
            written += compressed.len();
        }
    }

    Ok(written)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    use crate::{ArchiveVersion, CompressionAlgorithm, Format, TableOfContents, Timestamp};

    use super::{
        TableDataReader, read_blob_data, read_block_header, read_table_data, write_blob_block,
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

        let parsed = read_table_data(&mut buf, &toc).unwrap();
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

        let parsed = read_table_data(&mut buf, &toc).unwrap();
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

        let mut reader = TableDataReader::new(&mut buf, toc.int_size, toc.compression).unwrap();
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

        let mut reader = TableDataReader::new(&mut buf, toc.int_size, toc.compression).unwrap();
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

        let mut entry_reader = TableDataReader::new(&mut buf, toc.int_size, toc.compression).unwrap();
        match &mut entry_reader {
            TableDataReader::Raw(raw) => {
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
            TableDataReader::Compressed(_) => panic!("expected raw entry reader"),
        }
    }
}
