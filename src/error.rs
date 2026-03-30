use crate::version::ArchiveVersion;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid archive header: {0}")]
    InvalidHeader(String),

    #[error("unsupported archive version: {0}")]
    UnsupportedVersion(ArchiveVersion),

    #[error("unsupported format: {0}")]
    UnsupportedFormat(u8),

    #[error("unsupported compression algorithm: {0}")]
    UnsupportedCompression(u8),

    #[error("entity not found: {desc} {namespace}.{tag}")]
    EntityNotFound {
        desc: String,
        namespace: String,
        tag: String,
    },

    #[error("no data for entry with dump_id {0}")]
    NoData(i32),

    #[error("invalid dump ID: {0}")]
    InvalidDumpId(i32),

    #[error("data integrity error: {0}")]
    DataIntegrity(String),

    #[error("decompression error: {0}")]
    Decompression(String),

    #[error("invalid UTF-8 string: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
}

pub type Result<T> = std::result::Result<T, Error>;
