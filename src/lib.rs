pub mod compress;
pub mod constants;
pub mod dump;
pub mod entry;
pub mod error;
pub mod format;
pub mod io;
mod sort;
pub mod toc;
pub mod types;
pub mod version;

use std::path::Path;

pub use dump::Dump;
pub use entry::Entry;
pub use error::{Error, Result};
pub use format::custom::{
    CompressedEntryReader, CustomReader, EntryData, EntryReader, RawEntryReader,
};
pub use toc::TableOfContents;
pub use types::{
    Blob, BlockType, CompressionAlgorithm, Format, ObjectType, OffsetState, Section, Timestamp,
};
pub use version::ArchiveVersion;

/// Load a PostgreSQL custom format dump file.
pub fn load<P: AsRef<Path>>(path: P) -> Result<Dump> {
    Dump::load(path)
}

/// Create a new empty dump.
pub fn new(dbname: &str, encoding: &str, appear_as: &str) -> Result<Dump> {
    Dump::new(dbname, encoding, appear_as)
}
