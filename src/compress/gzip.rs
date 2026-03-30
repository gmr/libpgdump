use std::io::{Read, Write};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;

pub struct GzipDecompressor<R: Read> {
    inner: ZlibDecoder<R>,
}

impl<R: Read> GzipDecompressor<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: ZlibDecoder::new(reader),
        }
    }
}

impl<R: Read> Read for GzipDecompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

pub struct GzipCompressor<W: Write> {
    inner: Option<ZlibEncoder<W>>,
}

impl<W: Write> GzipCompressor<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: Some(ZlibEncoder::new(writer, flate2::Compression::default())),
        }
    }
}

impl<W: Write> Write for GzipCompressor<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.as_mut().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Finalize the zlib stream, writing the trailer and checksum.
        if let Some(encoder) = self.inner.take() {
            encoder.finish()?;
        }
        Ok(())
    }
}
