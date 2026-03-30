use std::io::{Read, Write};

pub struct NoneDecompressor<R: Read> {
    inner: R,
}

impl<R: Read> NoneDecompressor<R> {
    pub fn new(reader: R) -> Self {
        Self { inner: reader }
    }
}

impl<R: Read> Read for NoneDecompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

pub struct NoneCompressor<W: Write> {
    inner: W,
}

impl<W: Write> NoneCompressor<W> {
    pub fn new(writer: W) -> Self {
        Self { inner: writer }
    }
}

impl<W: Write> Write for NoneCompressor<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
