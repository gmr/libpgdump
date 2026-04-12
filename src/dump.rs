use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::format::custom;
use crate::format::directory;
use crate::format::tar;
use crate::header::Header;
use crate::sort;
use crate::types::{
    ArchiveData, Blob, CompressionAlgorithm, Format, ObjectType, OffsetState, Timestamp,
};
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
    pub(crate) blobs: HashMap<i32, Vec<Blob>>,
    next_dump_id: i32,
}

impl Dump {
    /// Load a dump from a file or directory.
    ///
    /// Automatically detects the format:
    /// - Directory → directory format (`-Fd`)
    /// - File starting with `PGDMP` → custom format (`-Fc`)
    /// - File with ustar header → tar format (`-Ft`)
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let archive = if path.is_dir() {
            directory::read_archive(path)?
        } else {
            match detect_file_format(path)? {
                Format::Tar => tar::read_archive(path)?,
                _ => {
                    let file = File::open(path)?;
                    let mut reader = BufReader::new(file);
                    custom::read_archive(&mut reader)?
                }
            }
        };
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
            blobs: HashMap::new(),
            next_dump_id: 1,
        };

        // Add standard initial entries like pgdumplib does
        dump.add_entry(
            ObjectType::Encoding,
            None,
            None,
            None,
            Some(&format!("SET client_encoding = '{encoding}';\n")),
            None,
            None,
            &[],
        )?;
        dump.add_entry(
            ObjectType::StdStrings,
            None,
            None,
            None,
            Some("SET standard_conforming_strings = 'on';\n"),
            None,
            None,
            &[],
        )?;
        dump.add_entry(
            ObjectType::SearchPath,
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

    pub(crate) fn from_archive_data(archive: ArchiveData) -> Self {
        let next_dump_id = archive.entries.iter().map(|e| e.dump_id).max().unwrap_or(0) + 1;
        Dump {
            header: archive.header,
            timestamp: archive.timestamp,
            dbname: archive.dbname,
            server_version: archive.server_version,
            dump_version: archive.dump_version,
            entries: archive.entries,
            data: archive.data,
            blobs: archive.blobs,
            next_dump_id,
        }
    }

    /// Save the dump to a file or directory.
    ///
    /// For custom format (`-Fc`), writes to a temporary file first, then
    /// atomically renames to avoid leaving a partial file on failure.
    /// For directory format (`-Fd`), writes directly to the directory.
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let archive = self.to_archive_data();

        if self.header.format == Format::Directory {
            directory::write_archive(path, &archive)
        } else if self.header.format == Format::Tar {
            tar::write_archive(path, &archive)
        } else {
            let tmp_path = path.with_extension("tmp");
            let result = (|| {
                let file = File::create(&tmp_path)?;
                let mut writer = BufWriter::new(file);
                custom::write_archive(&mut writer, &archive)?;
                writer
                    .into_inner()
                    .map_err(|e| Error::Io(e.into_error()))?
                    .sync_all()?;
                Ok(())
            })();
            match result {
                Ok(()) => {
                    std::fs::rename(&tmp_path, path)?;
                    Ok(())
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&tmp_path);
                    Err(e)
                }
            }
        }
    }

    /// Set the output format for writing.
    ///
    /// Note: Tar format does not support compression. If compression is set
    /// when switching to tar format, it will be reset to `None`.
    pub fn set_format(&mut self, format: Format) {
        if format == Format::Tar && self.header.compression != CompressionAlgorithm::None {
            self.header.compression = CompressionAlgorithm::None;
        }
        self.header.format = format;
    }

    fn to_archive_data(&self) -> ArchiveData {
        let mut entries = self.entries.clone();

        // Sort entries using weighted topological sort (matching pg_dump)
        sort::sort_entries(&mut entries);

        // For directory and tar formats, ensure entries have filenames
        if self.header.format == Format::Directory || self.header.format == Format::Tar {
            for entry in &mut entries {
                if entry.filename.is_none() {
                    if self.data.contains_key(&entry.dump_id) {
                        entry.filename = Some(format!("{}.dat", entry.dump_id));
                    } else if self.blobs.contains_key(&entry.dump_id) {
                        entry.filename = Some(format!("blobs_{}.toc", entry.dump_id));
                    }
                }
            }
        }

        ArchiveData {
            header: self.header.clone(),
            timestamp: self.timestamp.clone(),
            dbname: self.dbname.clone(),
            server_version: self.server_version.clone(),
            dump_version: self.dump_version.clone(),
            entries,
            data: self.data.clone(),
            blobs: self.blobs.clone(),
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

    /// Look up an entry by object type, namespace, and tag.
    pub fn lookup_entry(&self, desc: &ObjectType, namespace: &str, tag: &str) -> Option<&Entry> {
        self.entries.iter().find(|e| {
            e.desc == *desc
                && e.namespace.as_deref() == Some(namespace)
                && e.tag.as_deref() == Some(tag)
        })
    }

    /// Get an entry by dump_id.
    pub fn get_entry(&self, dump_id: i32) -> Option<&Entry> {
        self.entries.iter().find(|e| e.dump_id == dump_id)
    }

    /// Get a mutable reference to an entry by dump_id.
    pub fn get_entry_mut(&mut self, dump_id: i32) -> Option<&mut Entry> {
        self.entries.iter_mut().find(|e| e.dump_id == dump_id)
    }

    /// Iterate over table data rows for the given namespace and table.
    ///
    /// Each yielded item is a line from the COPY data (without the trailing newline).
    pub fn table_data(&self, namespace: &str, table: &str) -> Result<impl Iterator<Item = &str>> {
        let entry = self
            .entries
            .iter()
            .find(|e| {
                e.desc == ObjectType::TableData
                    && e.namespace.as_deref() == Some(namespace)
                    && e.tag.as_deref() == Some(table)
            })
            .ok_or_else(|| Error::EntityNotFound {
                desc: ObjectType::TableData,
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

    /// Iterate over all large objects (blobs) across all BLOBS entries.
    ///
    /// Yields `(oid, data)` pairs where `oid` is the large object OID and
    /// `data` is the decompressed blob content.
    pub fn blobs(&self) -> Vec<(i32, &[u8])> {
        let mut result = Vec::new();
        for entry in &self.entries {
            if (entry.desc == ObjectType::Blobs || entry.desc == ObjectType::Blob)
                && let Some(blob_list) = self.blobs.get(&entry.dump_id)
            {
                for blob in blob_list {
                    result.push((blob.oid, blob.data.as_slice()));
                }
            }
        }
        result
    }

    /// Add a large object (blob) to a BLOBS entry.
    ///
    /// If no BLOBS entry exists yet, one is created automatically.
    /// Returns the OID of the added blob.
    pub fn add_blob(&mut self, oid: i32, data: Vec<u8>) -> Result<i32> {
        // Find or create a BLOBS entry
        let blobs_dump_id = if let Some(entry) = self
            .entries
            .iter()
            .find(|e| e.desc == ObjectType::Blobs && e.had_dumper)
        {
            entry.dump_id
        } else {
            let dump_id =
                self.add_entry(ObjectType::Blobs, None, None, None, None, None, None, &[])?;
            let entry = self
                .entries
                .iter_mut()
                .find(|e| e.dump_id == dump_id)
                .unwrap();
            entry.had_dumper = true;
            entry.data_state = OffsetState::NotSet;
            dump_id
        };

        self.blobs
            .entry(blobs_dump_id)
            .or_default()
            .push(Blob { oid, data });
        Ok(oid)
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
        desc: ObjectType,
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

        let section = desc.section();
        let entry = Entry {
            dump_id,
            had_dumper: false,
            table_oid: "0".to_string(),
            oid: "0".to_string(),
            tag: tag.map(String::from),
            desc,
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
            filename: None,
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

    /// Sort TOC entries using the same weighted topological sort as pg_dump.
    ///
    /// Entries are first sorted by object-type priority (schema before table,
    /// table before index, etc.), then by namespace and name.  A topological
    /// sort pass then reorders only as needed to satisfy the dependency graph,
    /// preserving the cosmetic ordering wherever possible.
    ///
    /// This is called automatically by [`Dump::save`], but can also be called
    /// manually if you need the sorted order before writing.
    pub fn sort_entries(&mut self) {
        sort::sort_entries(&mut self.entries);
    }
}

/// Detect file format by reading magic bytes.
/// Custom format starts with "PGDMP"; tar has "ustar" at offset 257.
fn detect_file_format(path: &Path) -> Result<Format> {
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut buf = [0u8; 265];
    let n = file.read(&mut buf)?;

    if n >= 5 && &buf[..5] == b"PGDMP" {
        return Ok(Format::Custom);
    }
    if n >= 263 && &buf[257..263] == b"ustar\0" {
        return Ok(Format::Tar);
    }
    // GNU tar variant
    if n >= 265 && &buf[257..265] == b"ustar  \0" {
        return Ok(Format::Tar);
    }

    Ok(Format::Custom) // default fallback
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
