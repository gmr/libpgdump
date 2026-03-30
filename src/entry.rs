use crate::constants::section_for_desc;
use crate::types::{OffsetState, Section};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub dump_id: i32,
    pub had_dumper: bool,
    pub table_oid: String,
    pub oid: String,
    pub tag: Option<String>,
    pub desc: String,
    pub section: Section,
    pub defn: Option<String>,
    pub drop_stmt: Option<String>,
    pub copy_stmt: Option<String>,
    pub namespace: Option<String>,
    pub tablespace: Option<String>,
    pub tableam: Option<String>,
    pub relkind: Option<char>,
    pub owner: Option<String>,
    pub with_oids: bool,
    pub dependencies: Vec<i32>,
    pub data_state: OffsetState,
    pub offset: u64,
}

impl Entry {
    /// Returns the section this entry belongs to, derived from its description.
    pub fn default_section(&self) -> Section {
        section_for_desc(&self.desc)
    }
}
