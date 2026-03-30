use std::io::{Read, Write};

pub struct ZstdDecompressor<'a, R: Read> {
    inner: zstd::Decoder<'a, std::io::BufReader<R>>,
}

impl<'a, R: Read> ZstdDecompressor<'a, R> {
    pub fn new(reader: R) -> std::io::Result<Self> {
        Ok(Self {
            inner: zstd::Decoder::new(reader)?,
        })
    }
}

impl<R: Read> Read for ZstdDecompressor<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

/// Zstd compressor that finalizes the frame on drop.
pub struct ZstdCompressor<'a, W: Write> {
    inner: zstd::stream::AutoFinishEncoder<'a, W>,
}

impl<'a, W: Write> ZstdCompressor<'a, W> {
    pub fn new(writer: W) -> std::io::Result<Self> {
        let encoder = zstd::Encoder::new(writer, zstd::DEFAULT_COMPRESSION_LEVEL)?;
        Ok(Self {
            inner: encoder.auto_finish(),
        })
    }
}

impl<W: Write> Write for ZstdCompressor<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
