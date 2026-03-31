/// PostgreSQL dump object type.
///
/// Each variant corresponds to an object type string as stored in the TOC
/// (e.g. `"TABLE"`, `"INDEX"`, `"TABLE DATA"`).  The `Other` variant holds
/// unrecognised strings so that round-tripping never loses data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectType {
    AccessMethod,
    Acl,
    Aggregate,
    Blob,
    BlobMetadata,
    Blobs,
    Cast,
    CheckConstraint,
    Collation,
    Comment,
    Constraint,
    Conversion,
    Database,
    DatabaseProperties,
    Default,
    DefaultAcl,
    Domain,
    Encoding,
    EventTrigger,
    Extension,
    FkConstraint,
    ForeignDataWrapper,
    ForeignServer,
    ForeignTable,
    Function,
    Group,
    Index,
    IndexAttach,
    LargeObject,
    MaterializedView,
    MaterializedViewData,
    Operator,
    OperatorClass,
    OperatorFamily,
    PgLargeobject,
    PgLargeobjectMetadata,
    Policy,
    Procedure,
    ProceduralLanguage,
    Publication,
    PublicationTable,
    PublicationTablesInSchema,
    Role,
    RowSecurity,
    Rule,
    Schema,
    SearchPath,
    SecurityLabel,
    Sequence,
    SequenceOwnedBy,
    SequenceSet,
    Server,
    ShellType,
    Statistics,
    StatisticsData,
    StdStrings,
    Subscription,
    SubscriptionTable,
    Table,
    TableAttach,
    TableData,
    Tablespace,
    TextSearchConfiguration,
    TextSearchDictionary,
    TextSearchParser,
    TextSearchTemplate,
    Transform,
    Trigger,
    Type,
    User,
    UserMapping,
    View,
    /// Unrecognised object type string (preserved for round-tripping).
    Other(String),
}

impl ObjectType {
    /// The default section for this object type.
    pub fn section(&self) -> Section {
        match self {
            Self::AccessMethod
            | Self::Aggregate
            | Self::Blob
            | Self::BlobMetadata
            | Self::Cast
            | Self::Collation
            | Self::Conversion
            | Self::Database
            | Self::DatabaseProperties
            | Self::Default
            | Self::Domain
            | Self::Encoding
            | Self::Extension
            | Self::ForeignDataWrapper
            | Self::ForeignTable
            | Self::Function
            | Self::Operator
            | Self::OperatorClass
            | Self::OperatorFamily
            | Self::PgLargeobject
            | Self::PgLargeobjectMetadata
            | Self::Procedure
            | Self::ProceduralLanguage
            | Self::Schema
            | Self::SearchPath
            | Self::Sequence
            | Self::SequenceOwnedBy
            | Self::Server
            | Self::ShellType
            | Self::StdStrings
            | Self::Subscription
            | Self::Table
            | Self::Tablespace
            | Self::TextSearchConfiguration
            | Self::TextSearchDictionary
            | Self::TextSearchParser
            | Self::TextSearchTemplate
            | Self::Transform
            | Self::Type
            | Self::UserMapping
            | Self::View => Section::PreData,

            Self::Blobs | Self::SequenceSet | Self::TableData => Section::Data,

            Self::CheckConstraint
            | Self::Constraint
            | Self::DefaultAcl
            | Self::EventTrigger
            | Self::FkConstraint
            | Self::Index
            | Self::IndexAttach
            | Self::MaterializedView
            | Self::MaterializedViewData
            | Self::Policy
            | Self::PublicationTable
            | Self::PublicationTablesInSchema
            | Self::Publication
            | Self::RowSecurity
            | Self::Rule
            | Self::Statistics
            | Self::StatisticsData
            | Self::SubscriptionTable
            | Self::TableAttach
            | Self::Trigger => Section::PostData,

            Self::Acl
            | Self::Comment
            | Self::ForeignServer
            | Self::Group
            | Self::LargeObject
            | Self::Role
            | Self::SecurityLabel
            | Self::User => Section::None,

            Self::Other(_) => Section::None,
        }
    }

    /// Sort priority matching pg_dump's `dbObjectTypePriorities`.
    /// Lower numbers sort first.  Unrecognised types get 0.
    pub fn priority(&self) -> i32 {
        match self {
            Self::Encoding | Self::StdStrings | Self::SearchPath => 1,
            Self::Database | Self::DatabaseProperties => 2,
            Self::Schema => 3,
            Self::ProceduralLanguage => 4,
            Self::Collation => 5,
            Self::Transform => 6,
            Self::Extension => 7,
            Self::Type | Self::ShellType | Self::Domain => 8,
            Self::Cast => 9,
            Self::Function | Self::Procedure => 10,
            Self::Aggregate => 11,
            Self::AccessMethod => 12,
            Self::Operator => 13,
            Self::OperatorFamily | Self::OperatorClass => 14,
            Self::Conversion => 15,
            Self::TextSearchParser => 16,
            Self::TextSearchTemplate => 17,
            Self::TextSearchDictionary => 18,
            Self::TextSearchConfiguration => 19,
            Self::ForeignDataWrapper => 20,
            Self::ForeignServer | Self::Server => 21,
            Self::ForeignTable | Self::Table | Self::Sequence | Self::View => 22,
            Self::TableAttach => 23,
            Self::Default | Self::SequenceOwnedBy => 24,
            Self::TableData => 25,
            Self::SequenceSet => 26,
            Self::LargeObject | Self::Blob | Self::BlobMetadata | Self::PgLargeobjectMetadata => 27,
            Self::Blobs | Self::PgLargeobject => 28,
            Self::StatisticsData => 29,
            Self::CheckConstraint | Self::Constraint => 30,
            Self::Index => 31,
            Self::IndexAttach => 32,
            Self::Statistics => 33,
            Self::Rule => 34,
            Self::Trigger => 35,
            Self::FkConstraint => 36,
            Self::Policy | Self::RowSecurity => 37,
            Self::Publication => 38,
            Self::PublicationTable => 39,
            Self::PublicationTablesInSchema => 40,
            Self::Subscription => 41,
            Self::SubscriptionTable => 42,
            Self::DefaultAcl => 43,
            Self::MaterializedView => 44,
            Self::MaterializedViewData => 45,
            Self::EventTrigger => 46,
            Self::Acl | Self::Comment | Self::SecurityLabel => 47,
            Self::Group | Self::Role | Self::User | Self::UserMapping | Self::Tablespace => 48,
            Self::Other(_) => 0,
        }
    }
}

impl PartialOrd for ObjectType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ObjectType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority()
            .cmp(&other.priority())
            .then_with(|| self.as_str().cmp(other.as_str()))
    }
}

impl ObjectType {
    /// The archive string representation, without allocating for known variants.
    pub fn as_str(&self) -> &str {
        match self {
            Self::AccessMethod => "ACCESS METHOD",
            Self::Acl => "ACL",
            Self::Aggregate => "AGGREGATE",
            Self::Blob => "BLOB",
            Self::BlobMetadata => "BLOB METADATA",
            Self::Blobs => "BLOBS",
            Self::Cast => "CAST",
            Self::CheckConstraint => "CHECK CONSTRAINT",
            Self::Collation => "COLLATION",
            Self::Comment => "COMMENT",
            Self::Constraint => "CONSTRAINT",
            Self::Conversion => "CONVERSION",
            Self::Database => "DATABASE",
            Self::DatabaseProperties => "DATABASE PROPERTIES",
            Self::Default => "DEFAULT",
            Self::DefaultAcl => "DEFAULT ACL",
            Self::Domain => "DOMAIN",
            Self::Encoding => "ENCODING",
            Self::EventTrigger => "EVENT TRIGGER",
            Self::Extension => "EXTENSION",
            Self::FkConstraint => "FK CONSTRAINT",
            Self::ForeignDataWrapper => "FOREIGN DATA WRAPPER",
            Self::ForeignServer => "FOREIGN SERVER",
            Self::ForeignTable => "FOREIGN TABLE",
            Self::Function => "FUNCTION",
            Self::Group => "GROUP",
            Self::Index => "INDEX",
            Self::IndexAttach => "INDEX ATTACH",
            Self::LargeObject => "LARGE OBJECT",
            Self::MaterializedView => "MATERIALIZED VIEW",
            Self::MaterializedViewData => "MATERIALIZED VIEW DATA",
            Self::Operator => "OPERATOR",
            Self::OperatorClass => "OPERATOR CLASS",
            Self::OperatorFamily => "OPERATOR FAMILY",
            Self::PgLargeobject => "pg_largeobject",
            Self::PgLargeobjectMetadata => "pg_largeobject_metadata",
            Self::Policy => "POLICY",
            Self::Procedure => "PROCEDURE",
            Self::ProceduralLanguage => "PROCEDURAL LANGUAGE",
            Self::Publication => "PUBLICATION",
            Self::PublicationTable => "PUBLICATION TABLE",
            Self::PublicationTablesInSchema => "PUBLICATION TABLES IN SCHEMA",
            Self::Role => "ROLE",
            Self::RowSecurity => "ROW SECURITY",
            Self::Rule => "RULE",
            Self::Schema => "SCHEMA",
            Self::SearchPath => "SEARCHPATH",
            Self::SecurityLabel => "SECURITY LABEL",
            Self::Sequence => "SEQUENCE",
            Self::SequenceOwnedBy => "SEQUENCE OWNED BY",
            Self::SequenceSet => "SEQUENCE SET",
            Self::Server => "SERVER",
            Self::ShellType => "SHELL TYPE",
            Self::Statistics => "STATISTICS",
            Self::StatisticsData => "STATISTICS DATA",
            Self::StdStrings => "STDSTRINGS",
            Self::Subscription => "SUBSCRIPTION",
            Self::SubscriptionTable => "SUBSCRIPTION TABLE",
            Self::Table => "TABLE",
            Self::TableAttach => "TABLE ATTACH",
            Self::TableData => "TABLE DATA",
            Self::Tablespace => "TABLESPACE",
            Self::TextSearchConfiguration => "TEXT SEARCH CONFIGURATION",
            Self::TextSearchDictionary => "TEXT SEARCH DICTIONARY",
            Self::TextSearchParser => "TEXT SEARCH PARSER",
            Self::TextSearchTemplate => "TEXT SEARCH TEMPLATE",
            Self::Transform => "TRANSFORM",
            Self::Trigger => "TRIGGER",
            Self::Type => "TYPE",
            Self::User => "USER",
            Self::UserMapping => "USER MAPPING",
            Self::View => "VIEW",
            Self::Other(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for ObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl ObjectType {
    fn parse_known(s: &str) -> Option<Self> {
        Some(match s {
            "ACCESS METHOD" => Self::AccessMethod,
            "ACL" => Self::Acl,
            "AGGREGATE" => Self::Aggregate,
            "BLOB" => Self::Blob,
            "BLOB METADATA" => Self::BlobMetadata,
            "BLOBS" => Self::Blobs,
            "CAST" => Self::Cast,
            "CHECK CONSTRAINT" => Self::CheckConstraint,
            "COLLATION" => Self::Collation,
            "COMMENT" => Self::Comment,
            "CONSTRAINT" => Self::Constraint,
            "CONVERSION" => Self::Conversion,
            "DATABASE" => Self::Database,
            "DATABASE PROPERTIES" => Self::DatabaseProperties,
            "DEFAULT" => Self::Default,
            "DEFAULT ACL" => Self::DefaultAcl,
            "DOMAIN" => Self::Domain,
            "ENCODING" => Self::Encoding,
            "EVENT TRIGGER" => Self::EventTrigger,
            "EXTENSION" => Self::Extension,
            "FK CONSTRAINT" => Self::FkConstraint,
            "FOREIGN DATA WRAPPER" => Self::ForeignDataWrapper,
            "FOREIGN SERVER" => Self::ForeignServer,
            "FOREIGN TABLE" => Self::ForeignTable,
            "FUNCTION" => Self::Function,
            "GROUP" => Self::Group,
            "INDEX" => Self::Index,
            "INDEX ATTACH" => Self::IndexAttach,
            "LARGE OBJECT" => Self::LargeObject,
            "MATERIALIZED VIEW" => Self::MaterializedView,
            "MATERIALIZED VIEW DATA" => Self::MaterializedViewData,
            "OPERATOR" => Self::Operator,
            "OPERATOR CLASS" => Self::OperatorClass,
            "OPERATOR FAMILY" => Self::OperatorFamily,
            "pg_largeobject" => Self::PgLargeobject,
            "pg_largeobject_metadata" => Self::PgLargeobjectMetadata,
            "POLICY" => Self::Policy,
            "PROCEDURE" => Self::Procedure,
            "PROCEDURAL LANGUAGE" => Self::ProceduralLanguage,
            "PUBLICATION" => Self::Publication,
            "PUBLICATION TABLE" => Self::PublicationTable,
            "PUBLICATION TABLES IN SCHEMA" => Self::PublicationTablesInSchema,
            "ROLE" => Self::Role,
            "ROW SECURITY" => Self::RowSecurity,
            "RULE" => Self::Rule,
            "SCHEMA" => Self::Schema,
            "SEARCHPATH" => Self::SearchPath,
            "SECURITY LABEL" => Self::SecurityLabel,
            "SEQUENCE" => Self::Sequence,
            "SEQUENCE OWNED BY" => Self::SequenceOwnedBy,
            "SEQUENCE SET" => Self::SequenceSet,
            "SERVER" => Self::Server,
            "SHELL TYPE" => Self::ShellType,
            "STATISTICS" => Self::Statistics,
            "STATISTICS DATA" => Self::StatisticsData,
            "STDSTRINGS" => Self::StdStrings,
            "SUBSCRIPTION" => Self::Subscription,
            "SUBSCRIPTION TABLE" => Self::SubscriptionTable,
            "TABLE" => Self::Table,
            "TABLE ATTACH" => Self::TableAttach,
            "TABLE DATA" => Self::TableData,
            "TABLESPACE" => Self::Tablespace,
            "TEXT SEARCH CONFIGURATION" => Self::TextSearchConfiguration,
            "TEXT SEARCH DICTIONARY" => Self::TextSearchDictionary,
            "TEXT SEARCH PARSER" => Self::TextSearchParser,
            "TEXT SEARCH TEMPLATE" => Self::TextSearchTemplate,
            "TRANSFORM" => Self::Transform,
            "TRIGGER" => Self::Trigger,
            "TYPE" => Self::Type,
            "USER" => Self::User,
            "USER MAPPING" => Self::UserMapping,
            "VIEW" => Self::View,
            _ => return None,
        })
    }
}

impl From<&str> for ObjectType {
    fn from(s: &str) -> Self {
        Self::parse_known(s).unwrap_or_else(|| Self::Other(s.to_string()))
    }
}

impl From<String> for ObjectType {
    fn from(s: String) -> Self {
        Self::parse_known(s.as_str()).unwrap_or(Self::Other(s))
    }
}

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
