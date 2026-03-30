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
            Self::LargeObject | Self::Blob | Self::BlobMetadata => 27,
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
            Self::PgLargeobjectMetadata => 27,
            Self::Other(_) => 0,
        }
    }
}

impl std::fmt::Display for ObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AccessMethod => write!(f, "ACCESS METHOD"),
            Self::Acl => write!(f, "ACL"),
            Self::Aggregate => write!(f, "AGGREGATE"),
            Self::Blob => write!(f, "BLOB"),
            Self::BlobMetadata => write!(f, "BLOB METADATA"),
            Self::Blobs => write!(f, "BLOBS"),
            Self::Cast => write!(f, "CAST"),
            Self::CheckConstraint => write!(f, "CHECK CONSTRAINT"),
            Self::Collation => write!(f, "COLLATION"),
            Self::Comment => write!(f, "COMMENT"),
            Self::Constraint => write!(f, "CONSTRAINT"),
            Self::Conversion => write!(f, "CONVERSION"),
            Self::Database => write!(f, "DATABASE"),
            Self::DatabaseProperties => write!(f, "DATABASE PROPERTIES"),
            Self::Default => write!(f, "DEFAULT"),
            Self::DefaultAcl => write!(f, "DEFAULT ACL"),
            Self::Domain => write!(f, "DOMAIN"),
            Self::Encoding => write!(f, "ENCODING"),
            Self::EventTrigger => write!(f, "EVENT TRIGGER"),
            Self::Extension => write!(f, "EXTENSION"),
            Self::FkConstraint => write!(f, "FK CONSTRAINT"),
            Self::ForeignDataWrapper => write!(f, "FOREIGN DATA WRAPPER"),
            Self::ForeignServer => write!(f, "FOREIGN SERVER"),
            Self::ForeignTable => write!(f, "FOREIGN TABLE"),
            Self::Function => write!(f, "FUNCTION"),
            Self::Group => write!(f, "GROUP"),
            Self::Index => write!(f, "INDEX"),
            Self::IndexAttach => write!(f, "INDEX ATTACH"),
            Self::LargeObject => write!(f, "LARGE OBJECT"),
            Self::MaterializedView => write!(f, "MATERIALIZED VIEW"),
            Self::MaterializedViewData => write!(f, "MATERIALIZED VIEW DATA"),
            Self::Operator => write!(f, "OPERATOR"),
            Self::OperatorClass => write!(f, "OPERATOR CLASS"),
            Self::OperatorFamily => write!(f, "OPERATOR FAMILY"),
            Self::PgLargeobject => write!(f, "pg_largeobject"),
            Self::PgLargeobjectMetadata => write!(f, "pg_largeobject_metadata"),
            Self::Policy => write!(f, "POLICY"),
            Self::Procedure => write!(f, "PROCEDURE"),
            Self::ProceduralLanguage => write!(f, "PROCEDURAL LANGUAGE"),
            Self::Publication => write!(f, "PUBLICATION"),
            Self::PublicationTable => write!(f, "PUBLICATION TABLE"),
            Self::PublicationTablesInSchema => write!(f, "PUBLICATION TABLES IN SCHEMA"),
            Self::Role => write!(f, "ROLE"),
            Self::RowSecurity => write!(f, "ROW SECURITY"),
            Self::Rule => write!(f, "RULE"),
            Self::Schema => write!(f, "SCHEMA"),
            Self::SearchPath => write!(f, "SEARCHPATH"),
            Self::SecurityLabel => write!(f, "SECURITY LABEL"),
            Self::Sequence => write!(f, "SEQUENCE"),
            Self::SequenceOwnedBy => write!(f, "SEQUENCE OWNED BY"),
            Self::SequenceSet => write!(f, "SEQUENCE SET"),
            Self::Server => write!(f, "SERVER"),
            Self::ShellType => write!(f, "SHELL TYPE"),
            Self::Statistics => write!(f, "STATISTICS"),
            Self::StatisticsData => write!(f, "STATISTICS DATA"),
            Self::StdStrings => write!(f, "STDSTRINGS"),
            Self::Subscription => write!(f, "SUBSCRIPTION"),
            Self::SubscriptionTable => write!(f, "SUBSCRIPTION TABLE"),
            Self::Table => write!(f, "TABLE"),
            Self::TableAttach => write!(f, "TABLE ATTACH"),
            Self::TableData => write!(f, "TABLE DATA"),
            Self::Tablespace => write!(f, "TABLESPACE"),
            Self::TextSearchConfiguration => write!(f, "TEXT SEARCH CONFIGURATION"),
            Self::TextSearchDictionary => write!(f, "TEXT SEARCH DICTIONARY"),
            Self::TextSearchParser => write!(f, "TEXT SEARCH PARSER"),
            Self::TextSearchTemplate => write!(f, "TEXT SEARCH TEMPLATE"),
            Self::Transform => write!(f, "TRANSFORM"),
            Self::Trigger => write!(f, "TRIGGER"),
            Self::Type => write!(f, "TYPE"),
            Self::User => write!(f, "USER"),
            Self::UserMapping => write!(f, "USER MAPPING"),
            Self::View => write!(f, "VIEW"),
            Self::Other(s) => write!(f, "{s}"),
        }
    }
}

impl From<&str> for ObjectType {
    fn from(s: &str) -> Self {
        match s {
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
            other => Self::Other(other.to_string()),
        }
    }
}

impl From<String> for ObjectType {
    fn from(s: String) -> Self {
        // Try the known variants first, only allocate for Other
        match s.as_str() {
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
            _ => Self::Other(s),
        }
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
