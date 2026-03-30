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
    /// Custom format: offset state (set, not set, no data).
    pub data_state: OffsetState,
    /// Custom format: byte offset of this entry's data in the archive file.
    pub offset: u64,
    /// Directory/tar format: relative filename for this entry's data file.
    pub filename: Option<String>,
}

impl Entry {
    /// Computes the section from `self.desc` via [`section_for_desc`].
    ///
    /// This is a derived value and may differ from [`Entry::section`], which
    /// holds the section as read from the archive file.
    pub fn computed_section(&self) -> Section {
        section_for_desc(&self.desc)
    }
}
