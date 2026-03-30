use std::io::{Read, Write};

use lz4_flex::frame::{FrameDecoder, FrameEncoder};

pub struct Lz4Decompressor<R: Read> {
    inner: FrameDecoder<R>,
}

impl<R: Read> Lz4Decompressor<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: FrameDecoder::new(reader),
        }
    }
}

impl<R: Read> Read for Lz4Decompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

pub struct Lz4Compressor<W: Write> {
    inner: Option<FrameEncoder<W>>,
}

impl<W: Write> Lz4Compressor<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: Some(FrameEncoder::new(writer)),
        }
    }
}

impl<W: Write> Write for Lz4Compressor<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.as_mut().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Finalize the LZ4 frame, writing the frame footer.
        if let Some(encoder) = self.inner.take() {
            encoder.finish()?;
        }
        Ok(())
    }
}
