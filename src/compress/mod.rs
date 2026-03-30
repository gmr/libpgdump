pub mod gzip;
pub mod lz4;
pub mod none;
pub mod zstd;

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
        CompressionAlgorithm::Lz4 => Ok(Box::new(lz4::Lz4Decompressor::new(reader))),
        CompressionAlgorithm::Zstd => Ok(Box::new(
            zstd::ZstdDecompressor::new(reader).map_err(crate::error::Error::Io)?,
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
        CompressionAlgorithm::Lz4 => Ok(Box::new(lz4::Lz4Compressor::new(writer))),
        CompressionAlgorithm::Zstd => Ok(Box::new(
            zstd::ZstdCompressor::new(writer).map_err(crate::error::Error::Io)?,
        )),
    }
}
