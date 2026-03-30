use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use crate::constants;
use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::format::custom::{self, ArchiveData, Timestamp};
use crate::header::Header;
use crate::types::{CompressionAlgorithm, Format, OffsetState};
use crate::version::{self, ArchiveVersion};

/// A PostgreSQL dump archive.
#[derive(Debug)]
pub struct Dump {
    pub(crate) header: Header,
    pub(crate) timestamp: Timestamp,
    pub(crate) dbname: String,
    pub(crate) server_version: String,
    pub(crate) dump_version: String,
    pub(crate) entries: Vec<Entry>,
    pub(crate) data: HashMap<i32, Vec<u8>>,
    next_dump_id: i32,
}

impl Dump {
    /// Load a dump from a file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let archive = custom::read_archive(&mut reader)?;
        Ok(Self::from_archive_data(archive))
    }

    /// Create a new empty dump.
    pub fn new(dbname: &str, encoding: &str, appear_as: &str) -> Result<Self> {
        let (pg_major, pg_minor) = version::parse_pg_version(appear_as).unwrap_or((17, 0));
        let archive_version = version::pg_version_to_archive_version(pg_major, pg_minor)
            .unwrap_or(ArchiveVersion::new(1, 14, 0));

        let header = Header {
            version: archive_version,
            int_size: 4,
            off_size: 8,
            format: Format::Custom,
            compression: CompressionAlgorithm::None,
        };

        let now = now_timestamp();

        let mut dump = Dump {
            header,
            timestamp: now,
            dbname: dbname.to_string(),
            server_version: appear_as.to_string(),
            dump_version: format!("pg_dump (PostgreSQL) {appear_as}"),
            entries: Vec::new(),
            data: HashMap::new(),
            next_dump_id: 1,
        };

        // Add standard initial entries like pgdumplib does
        dump.add_entry(
            constants::ENCODING,
            None,
            None,
            None,
            Some(&format!("SET client_encoding = '{encoding}';\n")),
            None,
            None,
            &[],
        )?;
        dump.add_entry(
            constants::STDSTRINGS,
            None,
            None,
            None,
            Some("SET standard_conforming_strings = 'on';\n"),
            None,
            None,
            &[],
        )?;
        dump.add_entry(
            constants::SEARCHPATH,
            None,
            None,
            None,
            Some("SELECT pg_catalog.set_config('search_path', '', false);\n"),
            None,
            None,
            &[],
        )?;

        Ok(dump)
    }

    fn from_archive_data(archive: ArchiveData) -> Self {
        let next_dump_id = archive.entries.iter().map(|e| e.dump_id).max().unwrap_or(0) + 1;
        Dump {
            header: archive.header,
            timestamp: archive.timestamp,
            dbname: archive.dbname,
            server_version: archive.server_version,
            dump_version: archive.dump_version,
            entries: archive.entries,
            data: archive.data,
            next_dump_id,
        }
    }

    /// Save the dump to a file.
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        let archive = self.to_archive_data();
        custom::write_archive(&mut writer, &archive)?;
        Ok(())
    }

    fn to_archive_data(&self) -> ArchiveData {
        ArchiveData {
            header: self.header.clone(),
            timestamp: self.timestamp.clone(),
            dbname: self.dbname.clone(),
            server_version: self.server_version.clone(),
            dump_version: self.dump_version.clone(),
            entries: self.entries.clone(),
            data: self.data.clone(),
        }
    }

    // -- Accessors --

    /// The archive format version.
    pub fn version(&self) -> ArchiveVersion {
        self.header.version
    }

    /// The compression algorithm used.
    pub fn compression(&self) -> CompressionAlgorithm {
        self.header.compression
    }

    /// The database name.
    pub fn dbname(&self) -> &str {
        &self.dbname
    }

    /// The PostgreSQL server version string.
    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// The pg_dump version string.
    pub fn dump_version(&self) -> &str {
        &self.dump_version
    }

    /// The archive creation timestamp.
    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// All TOC entries.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Look up an entry by description, namespace, and tag.
    pub fn lookup_entry(&self, desc: &str, namespace: &str, tag: &str) -> Option<&Entry> {
        self.entries.iter().find(|e| {
            e.desc == desc
                && e.namespace.as_deref() == Some(namespace)
                && e.tag.as_deref() == Some(tag)
        })
    }

    /// Get an entry by dump_id.
    pub fn get_entry(&self, dump_id: i32) -> Option<&Entry> {
        self.entries.iter().find(|e| e.dump_id == dump_id)
    }

    /// Iterate over table data rows for the given namespace and table.
    ///
    /// Each yielded item is a line from the COPY data (without the trailing newline).
    pub fn table_data(&self, namespace: &str, table: &str) -> Result<impl Iterator<Item = &str>> {
        let entry = self
            .entries
            .iter()
            .find(|e| {
                e.desc == constants::TABLE_DATA
                    && e.namespace.as_deref() == Some(namespace)
                    && e.tag.as_deref() == Some(table)
            })
            .ok_or_else(|| Error::EntityNotFound {
                desc: constants::TABLE_DATA.to_string(),
                namespace: namespace.to_string(),
                tag: table.to_string(),
            })?;

        let data = self
            .data
            .get(&entry.dump_id)
            .ok_or(Error::NoData(entry.dump_id))?;

        let text = std::str::from_utf8(data)
            .map_err(|e| Error::DataIntegrity(format!("invalid UTF-8 in table data: {e}")))?;

        Ok(text
            .lines()
            .filter(|line| !line.is_empty() && *line != "\\."))
    }

    /// Iterate over large objects (blobs).
    ///
    /// Yields (oid, data) pairs.
    pub fn blobs(&self) -> Result<Vec<(String, &[u8])>> {
        let mut result = Vec::new();
        for entry in &self.entries {
            if (entry.desc == constants::BLOBS || entry.desc == constants::BLOB)
                && let Some(data) = self.data.get(&entry.dump_id)
            {
                let oid = entry.tag.as_deref().unwrap_or("0");
                result.push((oid.to_string(), data.as_slice()));
            }
        }
        Ok(result)
    }

    /// Get the raw data bytes for an entry by dump_id.
    pub fn entry_data(&self, dump_id: i32) -> Option<&[u8]> {
        self.data.get(&dump_id).map(|v| v.as_slice())
    }

    // -- Mutation --

    /// Add a new TOC entry.
    #[allow(clippy::too_many_arguments)]
    pub fn add_entry(
        &mut self,
        desc: &str,
        namespace: Option<&str>,
        tag: Option<&str>,
        owner: Option<&str>,
        defn: Option<&str>,
        drop_stmt: Option<&str>,
        copy_stmt: Option<&str>,
        dependencies: &[i32],
    ) -> Result<i32> {
        let dump_id = self.next_dump_id;
        self.next_dump_id += 1;

        let section = constants::section_for_desc(desc);
        let entry = Entry {
            dump_id,
            had_dumper: false,
            table_oid: "0".to_string(),
            oid: "0".to_string(),
            tag: tag.map(String::from),
            desc: desc.to_string(),
            section,
            defn: defn.map(String::from),
            drop_stmt: drop_stmt.map(String::from),
            copy_stmt: copy_stmt.map(String::from),
            namespace: namespace.map(String::from),
            tablespace: None,
            tableam: None,
            relkind: None,
            owner: owner.map(String::from),
            with_oids: false,
            dependencies: dependencies.to_vec(),
            data_state: OffsetState::NoData,
            offset: 0,
        };
        self.entries.push(entry);
        Ok(dump_id)
    }

    /// Set the data for an entry (raw COPY format bytes).
    pub fn set_entry_data(&mut self, dump_id: i32, data: Vec<u8>) -> Result<()> {
        let entry = self
            .entries
            .iter_mut()
            .find(|e| e.dump_id == dump_id)
            .ok_or(Error::InvalidDumpId(dump_id))?;
        entry.had_dumper = true;
        entry.data_state = OffsetState::NotSet;
        self.data.insert(dump_id, data);
        Ok(())
    }

    /// Set compression algorithm for writing.
    pub fn set_compression(&mut self, alg: CompressionAlgorithm) {
        self.header.compression = alg;
    }
}

fn now_timestamp() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // Convert epoch seconds to broken-down time (UTC).
    // pg_dump stores: sec, min, hour, mday, mon (0-based), year (since 1900), isdst.
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let second = (time_of_day % 60) as i32;
    let minute = ((time_of_day / 60) % 60) as i32;
    let hour = (time_of_day / 3600) as i32;

    // Days since 1970-01-01 -> year/month/day via civil_from_days algorithm
    let (year, month, day) = civil_from_days(days);

    Timestamp {
        second,
        minute,
        hour,
        day,
        month: month - 1,  // pg_dump uses 0-based months
        year: year - 1900, // pg_dump uses years since 1900
        is_dst: 0,
    }
}

/// Convert days since 1970-01-01 to (year, month, day).
/// Algorithm from Howard Hinnant's chrono-compatible date library.
fn civil_from_days(days: i64) -> (i32, i32, i32) {
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as i32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as i32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
