use crate::types::{CompressionAlgorithm, Format};
use crate::version::ArchiveVersion;

#[derive(Debug, Clone)]
pub struct Header {
    pub version: ArchiveVersion,
    pub int_size: u8,
    pub off_size: u8,
    pub format: Format,
    pub compression: CompressionAlgorithm,
}
