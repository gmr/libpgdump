use std::io::Write;
use std::path::Path;

use crate::error::{Error, Result};
use crate::format::custom::ArchiveData;
use crate::format::directory;
use crate::header::Header;
use crate::types::{CompressionAlgorithm, Format};

const TAR_BLOCK_SIZE: usize = 512;

/// Read a tar format archive from the given path.
pub fn read_archive(path: &Path) -> Result<ArchiveData> {
    let tar_data = std::fs::read(path)?;
    let members = parse_tar(&tar_data)?;

    // Extract toc.dat into a temporary directory structure and reuse
    // the directory format reader logic by writing members to a temp dir.
    let tmp = tempfile::TempDir::new().map_err(Error::Io)?;

    for member in &members {
        // Skip restore.sql — it's for human use only
        if member.name == "restore.sql" {
            continue;
        }
        // Reject paths that could escape the temp directory
        let member_path = std::path::Path::new(&member.name);
        let mut components = member_path.components();
        match (components.next(), components.next()) {
            (Some(std::path::Component::Normal(_)), None) => {}
            _ => {
                return Err(Error::DataIntegrity(format!(
                    "unsupported tar member path '{}'",
                    member.name
                )));
            }
        }
        let file_path = tmp.path().join(member_path);
        std::fs::write(&file_path, &member.data)?;
    }

    let mut archive = directory::read_archive(tmp.path())?;

    // Fix format to Tar
    archive.header = Header {
        format: Format::Tar,
        ..archive.header
    };

    Ok(archive)
}

/// Write a tar format archive to the given path.
pub fn write_archive(path: &Path, archive: &ArchiveData) -> Result<()> {
    // Tar format does not support compression
    if archive.header.compression != CompressionAlgorithm::None {
        return Err(Error::UnsupportedCompression(
            archive.header.compression as u8,
        ));
    }

    // Write to a temp directory first, then bundle into tar
    let tmp = tempfile::TempDir::new().map_err(Error::Io)?;

    // Write using directory format logic
    let dir_archive = ArchiveData {
        header: Header {
            format: Format::Tar, // toc.dat stores archTar
            ..archive.header.clone()
        },
        ..archive.clone()
    };
    directory::write_archive(tmp.path(), &dir_archive)?;

    // Bundle directory contents into tar
    let file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(file);

    // Add toc.dat first
    let toc_data = std::fs::read(tmp.path().join("toc.dat"))?;
    write_tar_member(&mut writer, "toc.dat", &toc_data)?;

    // Add data files and blob files
    let mut dir_entries: Vec<_> = std::fs::read_dir(tmp.path())?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name() != "toc.dat")
        .collect();
    dir_entries.sort_by_key(|e| e.file_name());

    for dir_entry in &dir_entries {
        let name = dir_entry.file_name().to_string_lossy().to_string();
        let data = std::fs::read(dir_entry.path())?;
        write_tar_member(&mut writer, &name, &data)?;
    }

    // Write restore.sql placeholder
    let restore_sql = b"-- This file is included for convenience.\n-- Edit $$PATH$$ references to point to extracted files.\n";
    write_tar_member(&mut writer, "restore.sql", restore_sql)?;

    // EOF marker: two blocks of NULLs
    writer.write_all(&[0u8; TAR_BLOCK_SIZE * 2])?;
    writer.flush()?;

    Ok(())
}

// -- Tar parsing --

struct TarMember {
    name: String,
    data: Vec<u8>,
}

/// Parse a tar archive into its member files.
fn parse_tar(data: &[u8]) -> Result<Vec<TarMember>> {
    let mut members = Vec::new();
    let mut pos = 0;

    while pos + TAR_BLOCK_SIZE <= data.len() {
        let header = &data[pos..pos + TAR_BLOCK_SIZE];

        // Two consecutive null blocks = end of archive
        if header.iter().all(|&b| b == 0) {
            break;
        }

        let name = parse_tar_name(header);
        if name.is_empty() {
            break;
        }

        let size = parse_tar_size(header)?;
        pos += TAR_BLOCK_SIZE;

        if pos + size > data.len() {
            return Err(Error::DataIntegrity(format!(
                "tar member '{}' extends past end of archive (size={}, pos={}, archive_len={})",
                name,
                size,
                pos,
                data.len()
            )));
        }

        let file_data = data[pos..pos + size].to_vec();
        members.push(TarMember {
            name,
            data: file_data,
        });

        // Advance past data + padding to next 512-byte boundary
        pos += size;
        let padding = (TAR_BLOCK_SIZE - (size % TAR_BLOCK_SIZE)) % TAR_BLOCK_SIZE;
        pos += padding;
    }

    Ok(members)
}

/// Extract the filename from a tar header.
fn parse_tar_name(header: &[u8]) -> String {
    let raw = &header[..100];
    let end = raw.iter().position(|&b| b == 0).unwrap_or(100);
    String::from_utf8_lossy(&raw[..end]).to_string()
}

/// Extract the file size from a tar header (octal ASCII at offset 124, 12 bytes).
fn parse_tar_size(header: &[u8]) -> Result<usize> {
    let raw = &header[124..136];
    let s = std::str::from_utf8(raw)
        .map_err(|_| Error::DataIntegrity("invalid tar size field".to_string()))?;
    let s = s.trim_matches(|c: char| c == '\0' || c == ' ');
    if s.is_empty() {
        return Ok(0);
    }
    usize::from_str_radix(s, 8)
        .map_err(|_| Error::DataIntegrity(format!("invalid tar size: '{s}'")))
}

// -- Tar writing --

/// Write a single tar member (header + data + padding).
fn write_tar_member<W: Write>(w: &mut W, name: &str, data: &[u8]) -> Result<()> {
    let header = build_tar_header(name, data.len())?;
    w.write_all(&header)?;
    w.write_all(data)?;

    // Pad to 512-byte boundary
    let padding = (TAR_BLOCK_SIZE - (data.len() % TAR_BLOCK_SIZE)) % TAR_BLOCK_SIZE;
    if padding > 0 {
        w.write_all(&vec![0u8; padding])?;
    }

    Ok(())
}

/// Build a 512-byte POSIX ustar tar header.
fn build_tar_header(name: &str, size: usize) -> Result<[u8; TAR_BLOCK_SIZE]> {
    let mut header = [0u8; TAR_BLOCK_SIZE];

    // Filename (offset 0, 100 bytes)
    let name_bytes = name.as_bytes();
    let len = name_bytes.len().min(99);
    header[..len].copy_from_slice(&name_bytes[..len]);

    // File mode (offset 100, 8 bytes) — 0600
    write_octal(&mut header[100..108], 0o600, 7)?;

    // UID (offset 108, 8 bytes)
    write_octal(&mut header[108..116], 0, 7)?;

    // GID (offset 116, 8 bytes)
    write_octal(&mut header[116..124], 0, 7)?;

    // File size (offset 124, 12 bytes)
    write_octal(&mut header[124..136], size, 11)?;

    // Modify time (offset 136, 12 bytes) — use 0
    write_octal(&mut header[136..148], 0, 11)?;

    // Type flag (offset 156, 1 byte) — '0' = regular file
    header[156] = b'0';

    // Magic (offset 257, 6 bytes) — "ustar\0"
    header[257..263].copy_from_slice(b"ustar\0");

    // Version (offset 263, 2 bytes) — "00"
    header[263..265].copy_from_slice(b"00");

    // Compute checksum (offset 148, 8 bytes)
    // Per POSIX: checksum is computed with the checksum field treated as spaces
    header[148..156].copy_from_slice(b"        ");
    let checksum: u32 = header.iter().map(|&b| b as u32).sum();
    write_octal(&mut header[148..156], checksum as usize, 6)?;
    header[154] = 0;
    header[155] = b' ';

    Ok(header)
}

/// Write a value as zero-padded octal ASCII into a tar header field.
fn write_octal(buf: &mut [u8], value: usize, width: usize) -> Result<()> {
    let s = format!("{value:0>width$o}", width = width);
    let bytes = s.as_bytes();
    if bytes.len() > buf.len() - 1 {
        return Err(Error::DataIntegrity(format!(
            "tar header field overflow: {value} does not fit in {} octal bytes",
            buf.len() - 1
        )));
    }
    buf[..bytes.len()].copy_from_slice(bytes);
    Ok(())
}

impl Clone for ArchiveData {
    fn clone(&self) -> Self {
        ArchiveData {
            header: self.header.clone(),
            timestamp: self.timestamp.clone(),
            dbname: self.dbname.clone(),
            server_version: self.server_version.clone(),
            dump_version: self.dump_version.clone(),
            entries: self.entries.clone(),
            data: self.data.clone(),
            blobs: self.blobs.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::entry::Entry;
    use crate::format::custom::{Blob, Timestamp};
    use crate::types::{ObjectType, OffsetState, Section};
    use crate::version::ArchiveVersion;

    fn make_test_header() -> Header {
        Header {
            version: ArchiveVersion::new(1, 14, 0),
            int_size: 4,
            off_size: 8,
            format: Format::Tar,
            compression: CompressionAlgorithm::None,
        }
    }

    #[test]
    fn test_tar_round_trip_with_data() {
        let data_content = b"1\tAlice\t30\n2\tBob\t25\n";

        let mut archive = ArchiveData {
            header: make_test_header(),
            timestamp: Timestamp {
                second: 0,
                minute: 0,
                hour: 0,
                day: 1,
                month: 0,
                year: 125,
                is_dst: 0,
            },
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
            entries: vec![Entry {
                dump_id: 1,
                had_dumper: true,
                table_oid: "16384".to_string(),
                oid: "0".to_string(),
                tag: Some("users".to_string()),
                desc: ObjectType::TableData,
                section: Section::Data,
                defn: None,
                drop_stmt: None,
                copy_stmt: Some("COPY public.users (id, name, age) FROM stdin;\n".to_string()),
                namespace: Some("public".to_string()),
                tablespace: None,
                tableam: None,
                relkind: None,
                owner: Some("postgres".to_string()),
                with_oids: false,
                dependencies: vec![],
                data_state: OffsetState::NotSet,
                offset: 0,
                filename: Some("1.dat".to_string()),
            }],
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        archive.data.insert(1, data_content.to_vec());

        let tmp = tempfile::NamedTempFile::new().unwrap();
        write_archive(tmp.path(), &archive).unwrap();

        let parsed = read_archive(tmp.path()).unwrap();
        assert_eq!(parsed.dbname, "testdb");
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.data.get(&1).unwrap(), data_content);
    }

    #[test]
    fn test_tar_round_trip_with_blobs() {
        let mut archive = ArchiveData {
            header: make_test_header(),
            timestamp: Timestamp {
                second: 0,
                minute: 0,
                hour: 0,
                day: 1,
                month: 0,
                year: 125,
                is_dst: 0,
            },
            dbname: "testdb".to_string(),
            server_version: "17.0".to_string(),
            dump_version: "pg_dump (PostgreSQL) 17.0".to_string(),
            entries: vec![Entry {
                dump_id: 1,
                had_dumper: true,
                table_oid: "0".to_string(),
                oid: "0".to_string(),
                tag: None,
                desc: ObjectType::Blobs,
                section: Section::Data,
                defn: None,
                drop_stmt: None,
                copy_stmt: None,
                namespace: None,
                tablespace: None,
                tableam: None,
                relkind: None,
                owner: None,
                with_oids: false,
                dependencies: vec![],
                data_state: OffsetState::NotSet,
                offset: 0,
                filename: Some("blobs_1.toc".to_string()),
            }],
            data: HashMap::new(),
            blobs: HashMap::new(),
        };
        archive.blobs.insert(
            1,
            vec![
                Blob {
                    oid: 100,
                    data: b"blob one".to_vec(),
                },
                Blob {
                    oid: 200,
                    data: b"blob two".to_vec(),
                },
            ],
        );

        let tmp = tempfile::NamedTempFile::new().unwrap();
        write_archive(tmp.path(), &archive).unwrap();

        let parsed = read_archive(tmp.path()).unwrap();
        let blobs = parsed.blobs.get(&1).unwrap();
        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].oid, 100);
        assert_eq!(blobs[0].data, b"blob one");
        assert_eq!(blobs[1].oid, 200);
        assert_eq!(blobs[1].data, b"blob two");
    }

    #[test]
    fn test_tar_header_checksum() {
        let header = build_tar_header("test.dat", 1024).unwrap();
        // Verify magic
        assert_eq!(&header[257..263], b"ustar\0");
        // Verify type flag
        assert_eq!(header[156], b'0');
    }

    #[test]
    fn test_parse_tar_size() {
        let mut header = [0u8; TAR_BLOCK_SIZE];
        // Write "00001750\0" at offset 124 (1000 in octal)
        header[124..133].copy_from_slice(b"00001750\0");
        assert_eq!(parse_tar_size(&header).unwrap(), 1000);
    }
}
