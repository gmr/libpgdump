use std::io::{Read, Seek};

use crate::error::{Error, Result};
use crate::{
    CompressionAlgorithm, Entry, Format, ObjectType, Section, TableOfContents,
    io::primitives::{
        read_byte, read_int, read_offset, read_string, read_timestamp, write_byte, write_int,
        write_offset, write_string, write_timestamp,
    },
    version::{ArchiveVersion, MAX_VERSION, MIN_VERSION},
};

pub const MAGIC: &[u8; 5] = b"PGDMP";

/// Read the header, timestamp, metadata strings, and all TOC entries.
/// Shared by `read_dump` (eager) and `CustomReader::open` (lazy).
pub fn read_toc<R: Read>(r: &mut R) -> Result<TableOfContents> {
    let mut magic = [0u8; 5];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(Error::InvalidHeader(format!(
            "invalid magic bytes: expected PGDMP, got {:?}",
            String::from_utf8_lossy(&magic)
        )));
    }

    // Read version
    let major = read_byte(r)?;
    let minor = read_byte(r)?;
    let rev = if major > 1 || (major == 1 && minor > 0) {
        read_byte(r)?
    } else {
        0
    };
    let version = ArchiveVersion::new(major, minor, rev);

    if version < MIN_VERSION || version > MAX_VERSION {
        return Err(Error::UnsupportedVersion(version));
    }

    let int_size = read_byte(r)?;
    if !(1..=8).contains(&int_size) {
        return Err(Error::InvalidHeader(format!(
            "invalid integer size: {int_size} (expected 1-8)"
        )));
    }

    // Offset size was added in v1.7
    let off_size = if version >= ArchiveVersion::new(1, 7, 0) {
        let s = read_byte(r)?;
        if !(1..=8).contains(&s) {
            return Err(Error::InvalidHeader(format!(
                "invalid offset size: {s} (expected 1-8)"
            )));
        }
        s
    } else {
        int_size
    };

    let format_byte = read_byte(r)?;
    let format = Format::from_byte(format_byte).ok_or(Error::UnsupportedFormat(format_byte))?;

    // Compression handling varies by version
    let compression = if version >= ArchiveVersion::new(1, 15, 0) {
        // v1.15+: explicit compression algorithm byte in header
        let comp_byte = read_byte(r)?;
        CompressionAlgorithm::from_byte(comp_byte)
            .ok_or(Error::UnsupportedCompression(comp_byte))?
    } else {
        // Pre-1.15: compression level integer follows; 0=none, >0=gzip
        let comp_level = read_int(r, int_size)?;
        if comp_level == 0 {
            CompressionAlgorithm::None
        } else {
            CompressionAlgorithm::Gzip
        }
    };

    let timestamp = read_timestamp(r, int_size)?;
    let dbname = read_string(r, int_size)?.unwrap_or_default();
    let server_version = read_string(r, int_size)?.unwrap_or_default();
    let dump_version = read_string(r, int_size)?.unwrap_or_default();

    let toc_count = read_int(r, int_size)?;
    if toc_count < 0 {
        return Err(Error::DataIntegrity(format!(
            "invalid TOC entry count: {toc_count}"
        )));
    }

    let mut toc = TableOfContents {
        version,
        int_size,
        off_size,
        format,
        compression,
        timestamp,
        dbname,
        server_version,
        dump_version,
        entries: Vec::with_capacity(toc_count as usize),
    };
    for _ in 0..toc_count {
        let entry = read_toc_entry(r, &toc)?;
        toc.entries.push(entry);
    }
    Ok(toc)
}

fn read_toc_entry<R: Read>(r: &mut R, toc: &TableOfContents) -> Result<Entry> {
    let int_size = toc.int_size;
    let off_size = toc.off_size;
    let version = toc.version;

    let dump_id = read_int(r, int_size)?;
    let had_dumper = read_int(r, int_size)? != 0;
    let table_oid = read_string(r, int_size)?.unwrap_or_else(|| "0".to_string());
    let oid = read_string(r, int_size)?.unwrap_or_else(|| "0".to_string());
    let tag = read_string(r, int_size)?;
    let desc: ObjectType = read_string(r, int_size)?
        .ok_or_else(|| Error::DataIntegrity("entry has no descriptor".into()))?
        .into();

    // Section integer is in the file (v>=1.11)
    let section = if version >= ArchiveVersion::new(1, 11, 0) {
        let sec_int = read_int(r, int_size)?;
        Section::from_int(sec_int).unwrap_or(Section::None)
    } else {
        Section::None
    };

    let defn = read_string(r, int_size)?;
    let drop_stmt = read_string(r, int_size)?;

    let copy_stmt = if version >= ArchiveVersion::new(1, 3, 0) {
        read_string(r, int_size)?
    } else {
        None
    };

    let namespace = if version >= ArchiveVersion::new(1, 6, 0) {
        read_string(r, int_size)?
    } else {
        None
    };

    let tablespace = if version >= ArchiveVersion::new(1, 10, 0) {
        read_string(r, int_size)?
    } else {
        None
    };

    let tableam = if version >= ArchiveVersion::new(1, 14, 0) {
        read_string(r, int_size)?
    } else {
        None
    };

    let relkind = if version >= ArchiveVersion::new(1, 16, 0) {
        let rk = read_int(r, int_size)?;
        if rk != 0 {
            char::from_u32(rk as u32)
        } else {
            None
        }
    } else {
        None
    };

    let owner = read_string(r, int_size)?;

    let with_oids = if version >= ArchiveVersion::new(1, 9, 0) {
        let s = read_string(r, int_size)?;
        s.as_deref() == Some("true")
    } else {
        false
    };

    // Dependencies: list of string dump IDs terminated by a NULL string
    let mut dependencies = Vec::new();
    if version >= ArchiveVersion::new(1, 5, 0) {
        loop {
            let dep_str = read_string(r, int_size)?;
            match dep_str {
                Some(s) if !s.is_empty() => {
                    if let Ok(dep_id) = s.parse::<i32>() {
                        dependencies.push(dep_id);
                    }
                }
                _ => break,
            }
        }
    }

    // Custom format extra TOC data: the data offset
    let (data_state, offset) = read_offset(r, off_size)?;

    Ok(Entry {
        dump_id,
        had_dumper,
        table_oid,
        oid,
        tag,
        desc,
        section,
        defn,
        drop_stmt,
        copy_stmt,
        namespace,
        tablespace,
        tableam,
        relkind,
        owner,
        with_oids,
        dependencies,
        data_state,
        offset,
        filename: None,
    })
}

pub fn write_toc<W: std::io::Write + std::io::Seek>(
    w: &mut W,
    toc: &TableOfContents,
) -> Result<Vec<u64>> {
    let int_size = toc.int_size;
    w.write_all(MAGIC)?;
    write_byte(w, toc.version.major)?;
    write_byte(w, toc.version.minor)?;
    write_byte(w, toc.version.rev)?;
    write_byte(w, int_size)?;

    if toc.version >= ArchiveVersion::new(1, 7, 0) {
        write_byte(w, toc.off_size)?;
    }

    write_byte(w, toc.format as u8)?;

    if toc.version >= ArchiveVersion::new(1, 15, 0) {
        write_byte(w, toc.compression as u8)?;
    } else {
        // Pre-1.15: only none and gzip are valid; write compression level
        let level = match toc.compression {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Gzip => 6,
            other => {
                return Err(Error::UnsupportedCompression(other as u8));
            }
        };
        write_int(w, level, int_size)?;
    }

    write_timestamp(w, &toc.timestamp, int_size)?;
    write_string(w, Some(&toc.dbname), int_size)?;
    write_string(w, Some(&toc.server_version), int_size)?;
    write_string(w, Some(&toc.dump_version), int_size)?;

    // Write entry count
    write_int(w, toc.entries.len() as i32, int_size)?;

    // Record the file positions of each entry's offset field for later fixup after writing data blocks
    let mut offset_positions = Vec::with_capacity(toc.entries.len());
    for entry in &toc.entries {
        offset_positions.push(write_toc_entry(w, entry, toc)?);
    }

    Ok(offset_positions)
}

/// Write an entry, returning the file position of the offset field (for later fixup).
fn write_toc_entry<W: std::io::Write + Seek>(
    w: &mut W,
    entry: &Entry,
    toc: &TableOfContents,
) -> Result<u64> {
    let int_size = toc.int_size;
    let off_size = toc.off_size;
    let version = toc.version;

    write_int(w, entry.dump_id, int_size)?;
    write_int(w, if entry.had_dumper { 1 } else { 0 }, int_size)?;
    write_string(w, Some(&entry.table_oid), int_size)?;
    write_string(w, Some(&entry.oid), int_size)?;
    write_string(w, entry.tag.as_deref(), int_size)?;
    write_string(w, Some(entry.desc.as_str()), int_size)?;

    if version >= ArchiveVersion::new(1, 11, 0) {
        write_int(w, entry.section.to_int(), int_size)?;
    }

    write_string(w, entry.defn.as_deref(), int_size)?;
    write_string(w, entry.drop_stmt.as_deref(), int_size)?;

    if version >= ArchiveVersion::new(1, 3, 0) {
        write_string(w, entry.copy_stmt.as_deref(), int_size)?;
    }

    if version >= ArchiveVersion::new(1, 6, 0) {
        write_string(w, entry.namespace.as_deref(), int_size)?;
    }

    if version >= ArchiveVersion::new(1, 10, 0) {
        write_string(w, entry.tablespace.as_deref(), int_size)?;
    }

    if version >= ArchiveVersion::new(1, 14, 0) {
        write_string(w, entry.tableam.as_deref(), int_size)?;
    }

    if version >= ArchiveVersion::new(1, 16, 0) {
        let rk = entry.relkind.map(|c| c as i32).unwrap_or(0);
        write_int(w, rk, int_size)?;
    }

    write_string(w, entry.owner.as_deref(), int_size)?;

    if version >= ArchiveVersion::new(1, 9, 0) {
        write_string(
            w,
            Some(if entry.with_oids { "true" } else { "false" }),
            int_size,
        )?;
    }

    if version >= ArchiveVersion::new(1, 5, 0) {
        for dep in &entry.dependencies {
            write_string(w, Some(&dep.to_string()), int_size)?;
        }
        // Terminate with NULL
        write_string(w, None, int_size)?;
    }

    // Record position of offset for later fixup
    let offset_pos = w.stream_position()?;
    write_offset(w, entry.data_state, entry.offset, off_size)?;

    Ok(offset_pos)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek, SeekFrom};

    use crate::{
        ArchiveVersion, CompressionAlgorithm, Entry, Format, ObjectType, OffsetState, Section,
        TableOfContents, Timestamp,
    };

    use super::{read_toc, write_toc};

    fn make_test_timestamp() -> Timestamp {
        Timestamp {
            second: 30,
            minute: 15,
            hour: 10,
            day: 25,
            month: 3,
            year: 2025,
            is_dst: 0,
        }
    }

    fn make_test_toc() -> TableOfContents {
        TableOfContents {
            version: ArchiveVersion::new(1, 14, 0),
            int_size: 4,
            off_size: 8,
            format: Format::Custom,
            compression: CompressionAlgorithm::None,
            timestamp: make_test_timestamp(),
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
            entries: Vec::new(),
        }
    }

    #[test]
    fn test_header_round_trip() {
        let header = make_test_toc();
        let mut buf = Cursor::new(Vec::new());
        write_toc(&mut buf, &header).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let parsed = read_toc(&mut buf).unwrap();
        assert_eq!(parsed, header);
    }

    #[test]
    fn test_toc_entry_round_trip() {
        let mut toc = make_test_toc();
        toc.entries.push(Entry {
            dump_id: 1,
            had_dumper: false,
            table_oid: "0".to_string(),
            oid: "0".to_string(),
            tag: Some("ENCODING".to_string()),
            desc: ObjectType::Encoding,
            section: Section::PreData,
            defn: Some("SET client_encoding = 'UTF8';\n".to_string()),
            drop_stmt: None,
            copy_stmt: None,
            namespace: None,
            tablespace: None,
            tableam: None,
            relkind: None,
            owner: None,
            with_oids: false,
            dependencies: vec![],
            data_state: OffsetState::NoData,
            offset: 0,
            filename: None,
        });

        let mut buf = Cursor::new(Vec::new());
        write_toc(&mut buf, &toc).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let parsed = read_toc(&mut buf).unwrap();

        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0], toc.entries[0]);
    }
}
