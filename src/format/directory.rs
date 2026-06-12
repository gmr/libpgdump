use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Read, Write};
use std::path::{Path, PathBuf};

use crate::compress;
use crate::constants::MAGIC;
use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::format::ArchiveMetadata;
use crate::format::custom::{ArchiveData, Blob, Timestamp};
use crate::header::Header;
use crate::io::primitives::{
    read_byte, read_int, read_string, write_byte, write_int, write_string,
};
use crate::types::{CompressionAlgorithm, Format, ObjectType, OffsetState, Section};
use crate::version::{ArchiveVersion, MAX_VERSION, MIN_VERSION};
use flate2::read::GzDecoder;

/// Read a directory format archive from the given path.
pub fn read_archive(dir: &Path) -> Result<ArchiveData> {
    let metadata = read_metadata(dir)?;

    // Read data files and blobs from the directory
    let (data, blobs) = read_data_files(dir, &metadata.header, &metadata.entries)?;

    Ok(ArchiveData {
        header: metadata.header,
        timestamp: metadata.timestamp,
        dbname: metadata.dbname,
        server_version: metadata.server_version,
        dump_version: metadata.dump_version,
        entries: metadata.entries,
        data,
        blobs,
    })
}

/// Read only archive metadata (header and TOC) from a directory archive.
pub fn read_metadata(dir: &Path) -> Result<ArchiveMetadata> {
    let toc_path = dir.join("toc.dat");
    if !toc_path.exists() {
        return Err(Error::InvalidHeader(format!(
            "toc.dat not found in {}",
            dir.display()
        )));
    }

    let toc_data = fs::read(&toc_path)?;
    read_toc_data(&toc_data, Format::Directory)
}

pub(crate) fn read_toc_data(toc_data: &[u8], format: Format) -> Result<ArchiveMetadata> {
    let mut r = Cursor::new(toc_data);

    let header = read_header(&mut r)?;
    let int_size = header.int_size;

    let timestamp = read_timestamp(&mut r, int_size)?;
    let dbname = read_string(&mut r, int_size)?.unwrap_or_default();
    let server_version = read_string(&mut r, int_size)?.unwrap_or_default();
    let dump_version = read_string(&mut r, int_size)?.unwrap_or_default();

    let toc_count = read_int(&mut r, int_size)?;
    if toc_count < 0 {
        return Err(Error::DataIntegrity(format!(
            "invalid TOC entry count: {toc_count}"
        )));
    }

    let mut entries = Vec::with_capacity(toc_count as usize);
    for _ in 0..toc_count {
        entries.push(read_entry(&mut r, &header)?);
    }

    Ok(ArchiveMetadata {
        // toc.dat stores archTar(3), override to caller format.
        header: Header { format, ..header },
        timestamp,
        dbname,
        server_version,
        dump_version,
        entries,
    })
}

/// Write a directory format archive to the given path.
pub fn write_archive(dir: &Path, archive: &ArchiveData) -> Result<()> {
    fs::create_dir_all(dir)?;

    // Write toc.dat (uses archTar format code per pg_dump convention)
    let toc_path = dir.join("toc.dat");
    let mut toc_file = BufWriter::new(fs::File::create(&toc_path)?);

    let toc_header = Header {
        format: Format::Tar, // directory format writes archTar in toc.dat
        ..archive.header.clone()
    };

    write_header(&mut toc_file, &toc_header)?;
    write_timestamp(&mut toc_file, &archive.timestamp, toc_header.int_size)?;
    write_string(&mut toc_file, Some(&archive.dbname), toc_header.int_size)?;
    write_string(
        &mut toc_file,
        Some(&archive.server_version),
        toc_header.int_size,
    )?;
    write_string(
        &mut toc_file,
        Some(&archive.dump_version),
        toc_header.int_size,
    )?;

    let entry_count: i32 = archive
        .entries
        .len()
        .try_into()
        .map_err(|_| Error::DataIntegrity("too many entries for i32".to_string()))?;
    write_int(&mut toc_file, entry_count, toc_header.int_size)?;

    for entry in &archive.entries {
        write_entry(&mut toc_file, entry, &toc_header)?;
    }

    // Write data files
    for entry in &archive.entries {
        if let Some(data) = archive.data.get(&entry.dump_id) {
            let filename = data_filename(entry.dump_id, archive.header.compression);
            let file_path = dir.join(&filename);
            write_data_file(&file_path, &archive.header, data)?;
        }
    }

    // Write blob files
    for entry in &archive.entries {
        if let Some(blob_list) = archive.blobs.get(&entry.dump_id) {
            write_blob_files(dir, &archive.header, entry.dump_id, blob_list)?;
        }
    }

    Ok(())
}

// -- Header reading/writing --

fn read_header<R: Read>(r: &mut R) -> Result<Header> {
    let mut magic = [0u8; 5];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(Error::InvalidHeader(format!(
            "invalid magic bytes: expected PGDMP, got {:?}",
            String::from_utf8_lossy(&magic)
        )));
    }

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
    // Directory format toc.dat stores archTar(3); accept both tar and directory
    let format = match format_byte {
        3 => Format::Tar, // will be overridden to Directory by caller
        5 => Format::Directory,
        _ => Format::from_byte(format_byte).ok_or(Error::UnsupportedFormat(format_byte))?,
    };

    let compression = if version >= ArchiveVersion::new(1, 15, 0) {
        let comp_byte = read_byte(r)?;
        CompressionAlgorithm::from_byte(comp_byte)
            .ok_or(Error::UnsupportedCompression(comp_byte))?
    } else {
        let comp_level = read_int(r, int_size)?;
        if comp_level == 0 {
            CompressionAlgorithm::None
        } else {
            CompressionAlgorithm::Gzip
        }
    };

    Ok(Header {
        version,
        int_size,
        off_size,
        format,
        compression,
    })
}

fn write_header<W: Write>(w: &mut W, header: &Header) -> Result<()> {
    w.write_all(MAGIC)?;
    write_byte(w, header.version.major)?;
    write_byte(w, header.version.minor)?;
    write_byte(w, header.version.rev)?;
    write_byte(w, header.int_size)?;

    if header.version >= ArchiveVersion::new(1, 7, 0) {
        write_byte(w, header.off_size)?;
    }

    write_byte(w, header.format as u8)?;

    if header.version >= ArchiveVersion::new(1, 15, 0) {
        write_byte(w, header.compression as u8)?;
    } else {
        let level = match header.compression {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Gzip => 6,
            other => {
                return Err(Error::UnsupportedCompression(other as u8));
            }
        };
        write_int(w, level, header.int_size)?;
    }

    Ok(())
}

fn read_timestamp<R: Read>(r: &mut R, int_size: u8) -> Result<Timestamp> {
    Ok(Timestamp {
        second: read_int(r, int_size)?,
        minute: read_int(r, int_size)?,
        hour: read_int(r, int_size)?,
        day: read_int(r, int_size)?,
        month: read_int(r, int_size)?,
        year: read_int(r, int_size)?,
        is_dst: read_int(r, int_size)?,
    })
}

fn write_timestamp<W: Write>(w: &mut W, ts: &Timestamp, int_size: u8) -> Result<()> {
    write_int(w, ts.second, int_size)?;
    write_int(w, ts.minute, int_size)?;
    write_int(w, ts.hour, int_size)?;
    write_int(w, ts.day, int_size)?;
    write_int(w, ts.month, int_size)?;
    write_int(w, ts.year, int_size)?;
    write_int(w, ts.is_dst, int_size)?;
    Ok(())
}

// -- Entry reading/writing --

fn read_entry<R: Read>(r: &mut R, header: &Header) -> Result<Entry> {
    let int_size = header.int_size;
    let version = header.version;

    let dump_id = read_int(r, int_size)?;
    let had_dumper = read_int(r, int_size)? != 0;
    let table_oid = read_string(r, int_size)?.unwrap_or_else(|| "0".to_string());
    let oid = read_string(r, int_size)?.unwrap_or_else(|| "0".to_string());
    let tag = read_string(r, int_size)?;
    let desc: ObjectType = read_string(r, int_size)?
        .ok_or_else(|| Error::DataIntegrity("entry has no descriptor".into()))?
        .into();

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

    // Directory format extra TOC data: filename string
    let filename = read_string(r, int_size)?;

    let data_state = if filename.is_some() {
        OffsetState::NotSet
    } else {
        OffsetState::NoData
    };

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
        offset: 0,
        filename,
    })
}

fn write_entry<W: Write>(w: &mut W, entry: &Entry, header: &Header) -> Result<()> {
    let int_size = header.int_size;
    let version = header.version;

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
        write_string(w, None, int_size)?;
    }

    // Directory format extra TOC data: filename
    write_string(w, entry.filename.as_deref(), int_size)?;

    Ok(())
}

// -- Data file reading/writing --

/// Try to find a data file with any compression extension.
fn find_data_file(dir: &Path, base_name: &str) -> Option<(PathBuf, CompressionAlgorithm)> {
    let candidates = [
        (base_name.to_string(), CompressionAlgorithm::None),
        (format!("{base_name}.gz"), CompressionAlgorithm::Gzip),
        (format!("{base_name}.lz4"), CompressionAlgorithm::Lz4),
        (format!("{base_name}.zst"), CompressionAlgorithm::Zstd),
    ];
    for (name, alg) in &candidates {
        let path = dir.join(name);
        if path.exists() {
            return Some((path, *alg));
        }
    }
    None
}

/// Read all data files and blobs from the directory.
#[allow(clippy::type_complexity)]
fn read_data_files(
    dir: &Path,
    header: &Header,
    entries: &[Entry],
) -> Result<(HashMap<i32, Vec<u8>>, HashMap<i32, Vec<Blob>>)> {
    let mut data_map: HashMap<i32, Vec<u8>> = HashMap::new();
    let mut blob_map: HashMap<i32, Vec<Blob>> = HashMap::new();

    for entry in entries {
        let filename = match &entry.filename {
            Some(f) if !f.is_empty() => f,
            _ => continue,
        };

        if entry.desc == ObjectType::Blobs {
            // Read blob TOC file and individual blob files
            let blobs = read_blob_toc(dir, header, filename)?;
            if !blobs.is_empty() {
                blob_map.insert(entry.dump_id, blobs);
            }
        } else {
            // Read data file (try with compression extensions)
            let base_name = filename.as_str();
            if let Some((file_path, file_compression)) = find_data_file(dir, base_name) {
                let raw = fs::read(&file_path)?;
                let data = decompress_file_data(&raw, file_compression)?;
                data_map.insert(entry.dump_id, data);
            }
        }
    }

    Ok((data_map, blob_map))
}

/// Parse a blobs TOC file and read individual blob data files.
fn read_blob_toc(dir: &Path, _header: &Header, toc_filename: &str) -> Result<Vec<Blob>> {
    let toc_path = dir.join(toc_filename);
    if !toc_path.exists() {
        return Ok(Vec::new());
    }

    let file = fs::File::open(&toc_path)?;
    let reader = BufReader::new(file);
    let mut blobs = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Format: "<oid> <filename>"
        let mut parts = line.splitn(2, ' ');
        let oid: i32 = parts
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| Error::DataIntegrity(format!("invalid blob TOC line: {line}")))?;
        let blob_filename = parts
            .next()
            .ok_or_else(|| Error::DataIntegrity(format!("missing blob filename: {line}")))?;

        if let Some((file_path, file_compression)) = find_data_file(dir, blob_filename) {
            let raw = fs::read(&file_path)?;
            let data = decompress_file_data(&raw, file_compression)?;
            blobs.push(Blob { oid, data });
        } else {
            return Err(Error::DataIntegrity(format!(
                "blob data file not found: {blob_filename} in {}",
                dir.display()
            )));
        }
    }

    Ok(blobs)
}

/// Decompress file data based on the detected compression algorithm.
///
/// Directory format files use gzip framing (not raw zlib), so we use
/// `GzDecoder` for `.gz` files and the standard compress module for others.
fn decompress_file_data(raw: &[u8], compression: CompressionAlgorithm) -> Result<Vec<u8>> {
    match compression {
        CompressionAlgorithm::None => Ok(raw.to_vec()),
        CompressionAlgorithm::Gzip => {
            let mut decoder = GzDecoder::new(raw);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            Ok(decompressed)
        }
        _ => {
            let cursor = Cursor::new(raw);
            let mut decompressor = compress::decompressor(compression, cursor)?;
            let mut decompressed = Vec::new();
            decompressor.read_to_end(&mut decompressed)?;
            Ok(decompressed)
        }
    }
}

/// Compute the data filename for a dump_id, with compression extension.
fn data_filename(dump_id: i32, compression: CompressionAlgorithm) -> String {
    let base = format!("{dump_id}.dat");
    match compression {
        CompressionAlgorithm::None => base,
        CompressionAlgorithm::Gzip => format!("{base}.gz"),
        CompressionAlgorithm::Lz4 => format!("{base}.lz4"),
        CompressionAlgorithm::Zstd => format!("{base}.zst"),
    }
}

/// Write a data file, applying compression.
///
/// Directory format files use gzip framing for `.gz` files (not raw zlib).
fn write_data_file(path: &Path, header: &Header, data: &[u8]) -> Result<()> {
    let file = fs::File::create(path)?;
    let mut writer = BufWriter::new(file);

    match header.compression {
        CompressionAlgorithm::None => {
            writer.write_all(data)?;
        }
        CompressionAlgorithm::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(&mut writer, flate2::Compression::default());
            encoder.write_all(data)?;
            encoder.finish()?;
        }
        _ => {
            let mut comp = compress::compressor(header.compression, &mut writer)?;
            comp.write_all(data)?;
            comp.flush()?;
        }
    }

    Ok(())
}

/// Write blob files and the blobs TOC file for a BLOBS entry.
fn write_blob_files(dir: &Path, header: &Header, dump_id: i32, blobs: &[Blob]) -> Result<()> {
    let toc_filename = format!("blobs_{dump_id}.toc");
    let toc_path = dir.join(&toc_filename);
    let mut toc_file = BufWriter::new(fs::File::create(&toc_path)?);

    for blob in blobs {
        let blob_base = format!("blob_{}.dat", blob.oid);
        let blob_filename = match header.compression {
            CompressionAlgorithm::None => blob_base.clone(),
            CompressionAlgorithm::Gzip => format!("{blob_base}.gz"),
            CompressionAlgorithm::Lz4 => format!("{blob_base}.lz4"),
            CompressionAlgorithm::Zstd => format!("{blob_base}.zst"),
        };

        // Write blob data file
        let blob_path = dir.join(&blob_filename);
        write_data_file(&blob_path, header, &blob.data)?;

        // Write TOC line: "<oid> <filename>"
        writeln!(toc_file, "{} {blob_base}", blob.oid)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OffsetState;

    fn make_test_header() -> Header {
        Header {
            version: ArchiveVersion::new(1, 14, 0),
            int_size: 4,
            off_size: 8,
            format: Format::Directory,
            compression: CompressionAlgorithm::None,
        }
    }

    #[test]
    fn test_directory_round_trip_no_data() {
        let archive = ArchiveData {
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
            }],
            data: HashMap::new(),
            blobs: HashMap::new(),
        };

        let tmp = tempfile::TempDir::new().unwrap();
        write_archive(tmp.path(), &archive).unwrap();

        let parsed = read_archive(tmp.path()).unwrap();
        assert_eq!(parsed.dbname, "testdb");
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].desc, ObjectType::Encoding);
    }

    #[test]
    fn test_directory_round_trip_unknown_desc() {
        let archive = ArchiveData {
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
                had_dumper: false,
                table_oid: "0".to_string(),
                oid: "0".to_string(),
                tag: Some("future_thing".to_string()),
                desc: ObjectType::Other("FUTURE TYPE".into()),
                section: Section::None,
                defn: Some("CREATE FUTURE TYPE future_thing;\n".to_string()),
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
            }],
            data: HashMap::new(),
            blobs: HashMap::new(),
        };

        let tmp = tempfile::TempDir::new().unwrap();
        write_archive(tmp.path(), &archive).unwrap();

        let parsed = read_archive(tmp.path()).unwrap();
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(
            parsed.entries[0].desc,
            ObjectType::Other("FUTURE TYPE".into())
        );
    }

    #[test]
    fn test_directory_round_trip_with_data() {
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

        let tmp = tempfile::TempDir::new().unwrap();
        write_archive(tmp.path(), &archive).unwrap();

        // Verify files exist
        assert!(tmp.path().join("toc.dat").exists());
        assert!(tmp.path().join("1.dat").exists());

        let parsed = read_archive(tmp.path()).unwrap();
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.data.get(&1).unwrap(), data_content);
    }

    #[test]
    fn test_directory_round_trip_with_blobs() {
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
                    oid: 16601,
                    data: b"blob content 1".to_vec(),
                },
                Blob {
                    oid: 16602,
                    data: b"blob content 2".to_vec(),
                },
            ],
        );

        let tmp = tempfile::TempDir::new().unwrap();
        write_archive(tmp.path(), &archive).unwrap();

        // Verify files exist
        assert!(tmp.path().join("blobs_1.toc").exists());
        assert!(tmp.path().join("blob_16601.dat").exists());
        assert!(tmp.path().join("blob_16602.dat").exists());

        let parsed = read_archive(tmp.path()).unwrap();
        let blobs = parsed.blobs.get(&1).unwrap();
        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].oid, 16601);
        assert_eq!(blobs[0].data, b"blob content 1");
        assert_eq!(blobs[1].oid, 16602);
        assert_eq!(blobs[1].data, b"blob content 2");
    }
}
