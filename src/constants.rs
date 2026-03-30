use crate::types::Section;

pub const MAGIC: &[u8; 5] = b"PGDMP";

// Object type constants
pub const ACCESS_METHOD: &str = "ACCESS METHOD";
pub const ACL: &str = "ACL";
pub const AGGREGATE: &str = "AGGREGATE";
pub const BLOB: &str = "BLOB";
pub const BLOB_METADATA: &str = "BLOB METADATA";
pub const BLOBS: &str = "BLOBS";
pub const CAST: &str = "CAST";
pub const CHECK_CONSTRAINT: &str = "CHECK CONSTRAINT";
pub const COLLATION: &str = "COLLATION";
pub const COMMENT: &str = "COMMENT";
pub const CONSTRAINT: &str = "CONSTRAINT";
pub const CONVERSION: &str = "CONVERSION";
pub const DATABASE: &str = "DATABASE";
pub const DATABASE_PROPERTIES: &str = "DATABASE PROPERTIES";
pub const DEFAULT: &str = "DEFAULT";
pub const DEFAULT_ACL: &str = "DEFAULT ACL";
pub const DOMAIN: &str = "DOMAIN";
pub const ENCODING: &str = "ENCODING";
pub const EVENT_TRIGGER: &str = "EVENT TRIGGER";
pub const EXTENSION: &str = "EXTENSION";
pub const FK_CONSTRAINT: &str = "FK CONSTRAINT";
pub const FOREIGN_DATA_WRAPPER: &str = "FOREIGN DATA WRAPPER";
pub const FOREIGN_SERVER: &str = "FOREIGN SERVER";
pub const FOREIGN_TABLE: &str = "FOREIGN TABLE";
pub const FUNCTION: &str = "FUNCTION";
pub const GROUP: &str = "GROUP";
pub const INDEX: &str = "INDEX";
pub const INDEX_ATTACH: &str = "INDEX ATTACH";
pub const LARGE_OBJECT: &str = "LARGE OBJECT";
pub const MATERIALIZED_VIEW: &str = "MATERIALIZED VIEW";
pub const MATERIALIZED_VIEW_DATA: &str = "MATERIALIZED VIEW DATA";
pub const OPERATOR: &str = "OPERATOR";
pub const OPERATOR_CLASS: &str = "OPERATOR CLASS";
pub const OPERATOR_FAMILY: &str = "OPERATOR FAMILY";
pub const PG_LARGEOBJECT: &str = "pg_largeobject";
pub const PG_LARGEOBJECT_METADATA: &str = "pg_largeobject_metadata";
pub const POLICY: &str = "POLICY";
pub const PROCEDURE: &str = "PROCEDURE";
pub const PROCEDURAL_LANGUAGE: &str = "PROCEDURAL LANGUAGE";
pub const PUBLICATION: &str = "PUBLICATION";
pub const PUBLICATION_TABLE: &str = "PUBLICATION TABLE";
pub const PUBLICATION_TABLES_IN_SCHEMA: &str = "PUBLICATION TABLES IN SCHEMA";
pub const ROLE: &str = "ROLE";
pub const ROW_SECURITY: &str = "ROW SECURITY";
pub const RULE: &str = "RULE";
pub const SCHEMA: &str = "SCHEMA";
pub const SEARCHPATH: &str = "SEARCHPATH";
pub const SECURITY_LABEL: &str = "SECURITY LABEL";
pub const SEQUENCE: &str = "SEQUENCE";
pub const SEQUENCE_OWNED_BY: &str = "SEQUENCE OWNED BY";
pub const SEQUENCE_SET: &str = "SEQUENCE SET";
pub const SERVER: &str = "SERVER";
pub const SHELL_TYPE: &str = "SHELL TYPE";
pub const STATISTICS: &str = "STATISTICS";
pub const STATISTICS_DATA: &str = "STATISTICS DATA";
pub const STDSTRINGS: &str = "STDSTRINGS";
pub const SUBSCRIPTION: &str = "SUBSCRIPTION";
pub const SUBSCRIPTION_TABLE: &str = "SUBSCRIPTION TABLE";
pub const TABLE: &str = "TABLE";
pub const TABLE_ATTACH: &str = "TABLE ATTACH";
pub const TABLE_DATA: &str = "TABLE DATA";
pub const TABLESPACE: &str = "TABLESPACE";
pub const TEXT_SEARCH_CONFIGURATION: &str = "TEXT SEARCH CONFIGURATION";
pub const TEXT_SEARCH_DICTIONARY: &str = "TEXT SEARCH DICTIONARY";
pub const TEXT_SEARCH_PARSER: &str = "TEXT SEARCH PARSER";
pub const TEXT_SEARCH_TEMPLATE: &str = "TEXT SEARCH TEMPLATE";
pub const TRANSFORM: &str = "TRANSFORM";
pub const TRIGGER: &str = "TRIGGER";
pub const TYPE: &str = "TYPE";
pub const USER: &str = "USER";
pub const USER_MAPPING: &str = "USER MAPPING";
pub const VIEW: &str = "VIEW";

/// Map an object type description to its default section.
pub fn section_for_desc(desc: &str) -> Section {
    match desc {
        ACCESS_METHOD
        | AGGREGATE
        | BLOB
        | BLOB_METADATA
        | CAST
        | COLLATION
        | CONVERSION
        | DATABASE
        | DATABASE_PROPERTIES
        | DEFAULT
        | DOMAIN
        | ENCODING
        | EXTENSION
        | FOREIGN_DATA_WRAPPER
        | FOREIGN_TABLE
        | FUNCTION
        | OPERATOR
        | OPERATOR_CLASS
        | OPERATOR_FAMILY
        | PG_LARGEOBJECT
        | PG_LARGEOBJECT_METADATA
        | PROCEDURE
        | PROCEDURAL_LANGUAGE
        | SCHEMA
        | SEARCHPATH
        | SEQUENCE
        | SEQUENCE_OWNED_BY
        | SERVER
        | SHELL_TYPE
        | STDSTRINGS
        | SUBSCRIPTION
        | TABLE
        | TABLESPACE
        | TEXT_SEARCH_CONFIGURATION
        | TEXT_SEARCH_DICTIONARY
        | TEXT_SEARCH_PARSER
        | TEXT_SEARCH_TEMPLATE
        | TRANSFORM
        | TYPE
        | USER_MAPPING
        | VIEW => Section::PreData,

        BLOBS | SEQUENCE_SET | TABLE_DATA => Section::Data,

        CHECK_CONSTRAINT
        | CONSTRAINT
        | DEFAULT_ACL
        | EVENT_TRIGGER
        | FK_CONSTRAINT
        | INDEX
        | INDEX_ATTACH
        | MATERIALIZED_VIEW
        | MATERIALIZED_VIEW_DATA
        | POLICY
        | PUBLICATION
        | PUBLICATION_TABLE
        | PUBLICATION_TABLES_IN_SCHEMA
        | ROW_SECURITY
        | RULE
        | STATISTICS
        | STATISTICS_DATA
        | SUBSCRIPTION_TABLE
        | TABLE_ATTACH
        | TRIGGER => Section::PostData,

        ACL | COMMENT | FOREIGN_SERVER | GROUP | LARGE_OBJECT | ROLE | SECURITY_LABEL | USER => {
            Section::None
        }

        _ => Section::None,
    }
}
