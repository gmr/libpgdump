use crate::types::{CompressionAlgorithm, Format};
use crate::version::ArchiveVersion;
use crate::{Entry, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOfContents {
    pub version: ArchiveVersion,
    pub int_size: u8,
    pub off_size: u8,
    pub format: Format,
    pub compression: CompressionAlgorithm,
    pub timestamp: Timestamp,
    pub dbname: String,
    pub server_version: String,
    pub dump_version: String,
    pub entries: Vec<Entry>,
}
