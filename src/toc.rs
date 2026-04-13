use crate::types::{CompressionAlgorithm, Format};
use crate::version::ArchiveVersion;
use crate::{Entry, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOfContents {
    /// The archive format version of this dump.
    pub version: ArchiveVersion,
    pub int_size: u8,
    pub off_size: u8,
    pub format: Format,
    pub compression: CompressionAlgorithm,
    pub timestamp: Timestamp,
    pub dbname: String,
    /// The PostgreSQL server version that created this dump, e.g. "17.0".
    pub server_version: String,
    /// The version of pg_dump that created this dump, e.g. "pg_dump (PostgreSQL) 17.0".
    pub dump_version: String,
    pub entries: Vec<Entry>,
}
