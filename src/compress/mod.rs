pub mod gzip;
pub mod none;

use std::io::{Read, Write};

use crate::error::Result;
use crate::types::CompressionAlgorithm;

/// Create a decompressing reader wrapping the given reader.
pub fn decompressor<'a, R: Read + 'a>(
    alg: CompressionAlgorithm,
    reader: R,
) -> Result<Box<dyn Read + 'a>> {
    match alg {
        CompressionAlgorithm::None => Ok(Box::new(none::NoneDecompressor::new(reader))),
        CompressionAlgorithm::Gzip => Ok(Box::new(gzip::GzipDecompressor::new(reader))),
        CompressionAlgorithm::Lz4 => Err(crate::error::Error::UnsupportedCompression(
            CompressionAlgorithm::Lz4 as u8,
        )),
        CompressionAlgorithm::Zstd => Err(crate::error::Error::UnsupportedCompression(
            CompressionAlgorithm::Zstd as u8,
        )),
    }
}

/// Create a compressing writer wrapping the given writer.
pub fn compressor<'a, W: Write + 'a>(
    alg: CompressionAlgorithm,
    writer: W,
) -> Result<Box<dyn Write + 'a>> {
    match alg {
        CompressionAlgorithm::None => Ok(Box::new(none::NoneCompressor::new(writer))),
        CompressionAlgorithm::Gzip => Ok(Box::new(gzip::GzipCompressor::new(writer))),
        CompressionAlgorithm::Lz4 => Err(crate::error::Error::UnsupportedCompression(
            CompressionAlgorithm::Lz4 as u8,
        )),
        CompressionAlgorithm::Zstd => Err(crate::error::Error::UnsupportedCompression(
            CompressionAlgorithm::Zstd as u8,
        )),
    }
}
