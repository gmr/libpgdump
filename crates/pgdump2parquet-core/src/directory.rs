//! pg_dump **directory format** (`-Fd`) input, without modifying libpgdump.
//!
//! A `-Fd` dump is a directory containing:
//!
//! * `toc.dat` — a binary table-of-contents in the **same PGDMP header + TOC
//!   format** as a custom-format (`-Fc`) archive, just without the inline
//!   data blocks.
//! * `<dump_id>.dat[.gz|.lz4|.zst]` — one compressed file per table, each
//!   containing a plain gzip/lz4/zstd stream of COPY-TEXT rows. There is **no**
//!   internal custom-format chunking inside these files — it's just raw
//!   compressed COPY text.
//! * `blobs_*.toc` — blob manifests (we ignore these; the tool converts
//!   TABLE DATA only).
//!
//! The trick this module exploits: libpgdump's `CustomReader::open` happily
//! parses `toc.dat` because the byte layout is the same up to the point
//! where data blocks would start. We never call `read_entry_stream` on the
//! toc.dat reader — we just pull `entries()` out of it and then open the
//! per-table files directly with a small decompressor factory.
//!
//! Net effect: full `-Fd` support with **zero modifications to the base
//! libpgdump crate**.

use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::{Path, PathBuf};

use libpgdump::constants::MAGIC;
use libpgdump::entry::Entry;
use libpgdump::header::Header;
use libpgdump::io::primitives::{read_byte, read_int, read_string};
use libpgdump::types::{CompressionAlgorithm, Format, ObjectType, OffsetState, Section};
use libpgdump::version::ArchiveVersion;

/// Errors specific to directory-format input.
#[derive(Debug, thiserror::Error)]
pub enum DirectoryError {
    #[error("libpgdump error: {0}")]
    Dump(#[from] libpgdump::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("zstd decoder error: {0}")]
    Zstd(std::io::Error),
    #[error(
        "entry {dump_id} refers to file {filename} but it does not exist under {dir}"
    )]
    MissingDataFile {
        dump_id: i32,
        filename: String,
        dir: String,
    },
}

/// A parsed directory-format dump — TOC in memory, per-table files on disk.
/// Cheap to construct (only `toc.dat` is read) and safe to share across
/// worker threads.
#[derive(Debug, Clone)]
pub struct DirectoryInput {
    dir: PathBuf,
    compression: CompressionAlgorithm,
    entries: Vec<Entry>,
    dbname: String,
    server_version: String,
    dump_version: String,
}

impl DirectoryInput {
    /// Open a `-Fd` dump directory, parsing `toc.dat` only. No per-table
    /// files are touched here.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, DirectoryError> {
        let dir = dir.as_ref().to_path_buf();
        let toc_path = dir.join("toc.dat");
        let toc_bytes = std::fs::read(&toc_path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => std::io::Error::new(
                e.kind(),
                format!("toc.dat not found in {}", dir.display()),
            ),
            _ => e,
        })?;
        let mut r = Cursor::new(toc_bytes);

        // Directory format's `toc.dat` uses the same PGDMP header as `-Fc`,
        // but each entry ends with a **filename string** instead of an
        // offset — so we parse it with libpgdump's public low-level
        // primitives rather than going through `CustomReader`. libpgdump
        // stays completely unmodified.
        let header = read_toc_header(&mut r)?;
        let int_size = header.int_size;

        // Timestamp: 7 ints (sec, min, hour, day, month, year, isdst).
        for _ in 0..7 {
            read_int(&mut r, int_size)?;
        }
        let dbname = read_string(&mut r, int_size)?.unwrap_or_default();
        let server_version = read_string(&mut r, int_size)?.unwrap_or_default();
        let dump_version = read_string(&mut r, int_size)?.unwrap_or_default();

        let toc_count = read_int(&mut r, int_size)?;
        if toc_count < 0 {
            return Err(DirectoryError::Dump(libpgdump::Error::DataIntegrity(
                format!("invalid TOC entry count: {toc_count}"),
            )));
        }
        let mut entries = Vec::with_capacity(toc_count as usize);
        for _ in 0..toc_count {
            entries.push(read_directory_entry(&mut r, &header)?);
        }

        Ok(Self {
            dir,
            compression: header.compression,
            entries,
            dbname,
            server_version,
            dump_version,
        })
    }

    /// The dump's compression algorithm, read from the `toc.dat` header.
    /// All per-table data files use this algorithm uniformly.
    pub fn compression(&self) -> CompressionAlgorithm {
        self.compression
    }

    /// All TOC entries (same shape as `CustomReader::entries()`).
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn dbname(&self) -> &str {
        &self.dbname
    }

    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    pub fn dump_version(&self) -> &str {
        &self.dump_version
    }

    /// Open the decompressed COPY-TEXT stream for one TABLE DATA entry.
    ///
    /// Returns `Ok(None)` if the entry has no data file (e.g. an entry with
    /// `had_dumper = false` or a non-data entry).
    ///
    /// The returned `Box<dyn Read>` decompresses the `.dat[.gz|.lz4|.zst]`
    /// file on the fly — single-threaded, bounded memory, matches pg_dump's
    /// layout exactly. Feed it to [`crate::drive_stream`].
    pub fn open_entry_stream(
        &self,
        dump_id: i32,
    ) -> Result<Option<Box<dyn Read + Send>>, DirectoryError> {
        let entry = match self.entries.iter().find(|e| e.dump_id == dump_id) {
            Some(e) => e,
            None => return Ok(None),
        };
        let Some(filename) = entry.filename.as_ref() else {
            return Ok(None);
        };
        if !entry.had_dumper {
            return Ok(None);
        }

        // pg_dump stores the *bare* filename (e.g. `3457.dat`) in `toc.dat`
        // and the compression suffix is implicit from the archive header.
        // Try the literal filename first, then with the compression suffix
        // appended — that covers both the pg_dump writer convention and
        // libpgdump's own convention (which stores the full name including
        // `.gz`/`.lz4`/`.zst`).
        let path = {
            let literal = self.dir.join(filename);
            if literal.exists() {
                literal
            } else {
                let suffix = match self.compression {
                    CompressionAlgorithm::None => "",
                    CompressionAlgorithm::Gzip => ".gz",
                    CompressionAlgorithm::Lz4 => ".lz4",
                    CompressionAlgorithm::Zstd => ".zst",
                };
                let with_suffix = self.dir.join(format!("{filename}{suffix}"));
                if with_suffix.exists() {
                    with_suffix
                } else {
                    return Err(DirectoryError::MissingDataFile {
                        dump_id,
                        filename: filename.clone(),
                        dir: self.dir.display().to_string(),
                    });
                }
            }
        };

        let file = File::open(&path)?;
        let buf = BufReader::with_capacity(1024 * 1024, file);

        // Infer compression from the header value pg_dump wrote; we could
        // also peek magic bytes but it's unnecessary — all per-table files
        // share the header's compression setting.
        let stream: Box<dyn Read + Send> = match self.compression {
            CompressionAlgorithm::None => Box::new(buf),
            CompressionAlgorithm::Gzip => Box::new(flate2::read::GzDecoder::new(buf)),
            CompressionAlgorithm::Zstd => {
                Box::new(zstd::stream::read::Decoder::new(buf).map_err(DirectoryError::Zstd)?)
            }
            CompressionAlgorithm::Lz4 => Box::new(lz4_flex::frame::FrameDecoder::new(buf)),
        };
        Ok(Some(stream))
    }

    /// Approximate compressed size of a table's data file. Used for
    /// largest-first job scheduling; returns 0 if the file doesn't exist or
    /// the entry has no filename.
    pub fn data_file_size(&self, dump_id: i32) -> u64 {
        let Some(entry) = self.entries.iter().find(|e| e.dump_id == dump_id) else {
            return 0;
        };
        let Some(filename) = entry.filename.as_ref() else {
            return 0;
        };
        // Try the literal filename, then the same filename with the
        // archive's compression suffix appended.
        let literal = self.dir.join(filename);
        if let Ok(m) = std::fs::metadata(&literal) {
            return m.len();
        }
        let suffix = match self.compression {
            CompressionAlgorithm::None => "",
            CompressionAlgorithm::Gzip => ".gz",
            CompressionAlgorithm::Lz4 => ".lz4",
            CompressionAlgorithm::Zstd => ".zst",
        };
        std::fs::metadata(self.dir.join(format!("{filename}{suffix}")))
            .map(|m| m.len())
            .unwrap_or(0)
    }
}

/// Lightweight TOC-entry snapshot exposed in the public API so callers
/// don't need to depend on libpgdump's `Entry` directly.
#[derive(Debug, Clone)]
pub struct TocEntry {
    pub dump_id: i32,
    pub namespace: Option<String>,
    pub tag: Option<String>,
    pub had_dumper: bool,
    pub filename: Option<String>,
}

impl From<&Entry> for TocEntry {
    fn from(e: &Entry) -> Self {
        Self {
            dump_id: e.dump_id,
            namespace: e.namespace.clone(),
            tag: e.tag.clone(),
            had_dumper: e.had_dumper,
            filename: e.filename.clone(),
        }
    }
}

/// Parse the PGDMP header from a `toc.dat`. Mirrors libpgdump's internal
/// `read_header` but only using the public primitives, so no lib mods are
/// needed. Kept in sync with archive versions 1.12 through 1.16.
fn read_toc_header<R: Read>(r: &mut R) -> Result<Header, DirectoryError> {
    let mut magic = [0u8; 5];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(DirectoryError::Dump(libpgdump::Error::InvalidHeader(
            format!(
                "invalid magic bytes in toc.dat: expected PGDMP, got {:?}",
                String::from_utf8_lossy(&magic)
            ),
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

    let int_size = read_byte(r)?;
    let off_size = if version >= ArchiveVersion::new(1, 7, 0) {
        read_byte(r)?
    } else {
        int_size
    };
    let format_byte = read_byte(r)?;
    let format = Format::from_byte(format_byte).unwrap_or(Format::Tar);

    let compression = if version >= ArchiveVersion::new(1, 15, 0) {
        let comp_byte = read_byte(r)?;
        CompressionAlgorithm::from_byte(comp_byte).unwrap_or(CompressionAlgorithm::None)
    } else {
        // Pre-1.15: compression level int; 0=none, >0=gzip.
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

/// Parse one directory-format TOC entry. Differs from custom-format entries
/// only at the tail: directory entries end with a **filename string**
/// (which points at `<dump_id>.dat[.gz|.lz4|.zst]`), whereas custom entries
/// end with an offset. The data_state is derived from whether a filename
/// is present.
fn read_directory_entry<R: Read>(r: &mut R, header: &Header) -> Result<Entry, DirectoryError> {
    let int_size = header.int_size;
    let version = header.version;

    let dump_id = read_int(r, int_size)?;
    let had_dumper = read_int(r, int_size)? != 0;
    let table_oid = read_string(r, int_size)?.unwrap_or_else(|| "0".to_string());
    let oid = read_string(r, int_size)?.unwrap_or_else(|| "0".to_string());
    let tag = read_string(r, int_size)?;
    let desc: ObjectType = read_string(r, int_size)?
        .ok_or_else(|| {
            DirectoryError::Dump(libpgdump::Error::DataIntegrity(
                "entry has no descriptor".into(),
            ))
        })?
        .into();

    let section = if version >= ArchiveVersion::new(1, 11, 0) {
        // The int is written on disk but `Section::from_int` is pub(crate).
        // We don't need the exact stored section — `desc.section()` gives
        // the canonical one for each ObjectType, which is what we want for
        // our driver anyway. Consume the int to advance the reader.
        let _sec_int = read_int(r, int_size)?;
        Section::None
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
        if rk != 0 { char::from_u32(rk as u32) } else { None }
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

    // Directory-format tail: filename string pointing at the per-table file.
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
