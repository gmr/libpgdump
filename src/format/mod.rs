use crate::entry::Entry;
use crate::format::custom::Timestamp;
use crate::header::Header;

pub mod custom;
pub mod directory;
pub mod tar;

/// Header and TOC metadata without entry data blocks.
#[derive(Debug, Clone)]
pub struct ArchiveMetadata {
    pub header: Header,
    pub timestamp: Timestamp,
    pub dbname: String,
    pub server_version: String,
    pub dump_version: String,
    pub entries: Vec<Entry>,
}
