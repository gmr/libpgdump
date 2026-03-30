#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Section {
    None,
    PreData,
    Data,
    PostData,
}

impl Section {
    pub(crate) fn from_int(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::None),
            2 => Some(Self::PreData),
            3 => Some(Self::Data),
            4 => Some(Self::PostData),
            _ => None,
        }
    }

    pub(crate) fn to_int(self) -> i32 {
        match self {
            Self::None => 1,
            Self::PreData => 2,
            Self::Data => 3,
            Self::PostData => 4,
        }
    }
}

impl std::fmt::Display for Section {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::PreData => write!(f, "Pre-Data"),
            Self::Data => write!(f, "DATA"),
            Self::PostData => write!(f, "Post-Data"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Unknown = 0,
    Custom = 1,
    Files = 2,
    Tar = 3,
    Null = 4,
    Directory = 5,
}

impl Format {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Unknown),
            1 => Some(Self::Custom),
            2 => Some(Self::Files),
            3 => Some(Self::Tar),
            4 => Some(Self::Null),
            5 => Some(Self::Directory),
            _ => None,
        }
    }
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Custom => write!(f, "Custom"),
            Self::Files => write!(f, "Files"),
            Self::Tar => write!(f, "Tar"),
            Self::Null => write!(f, "Null"),
            Self::Directory => write!(f, "Directory"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    None = 0,
    Gzip = 1,
    Lz4 = 2,
    Zstd = 3,
}

impl CompressionAlgorithm {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Lz4),
            3 => Some(Self::Zstd),
            _ => None,
        }
    }
}

impl std::fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Gzip => write!(f, "gzip"),
            Self::Lz4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockType {
    Data = 1,
    Blobs = 3,
}

impl BlockType {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Data),
            3 => Some(Self::Blobs),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetState {
    NotSet = 1,
    Set = 2,
    NoData = 3,
}

impl OffsetState {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::NotSet),
            2 => Some(Self::Set),
            3 => Some(Self::NoData),
            _ => None,
        }
    }
}
