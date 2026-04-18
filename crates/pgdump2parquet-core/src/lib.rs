//! Backend-agnostic machinery for converting a PostgreSQL `pg_dump` custom
//! format archive to Parquet files, one per table.
//!
//! The crate is split into three responsibilities:
//!
//! * [`copy_text`] — parser for PostgreSQL `COPY ... TO ... TEXT` rows.
//! * [`ddl`] — tiny `CREATE TABLE` parser over the very regular DDL pg_dump
//!   emits, extracting columns and types.
//! * [`sink`] — the [`ParquetSink`] / [`ParquetSinkFactory`] traits that
//!   concrete backends (arrow-rs, embedded DuckDB, ...) implement.
//!
//! The [`drive_table`] function ties these together: it streams one TABLE
//! DATA entry from a [`libpgdump::format::custom::CustomReader`] into a
//! caller-supplied sink.

pub mod copy_text;
pub mod ddl;
pub mod sink;

use std::io::{BufRead, BufReader, Read, Seek};

use libpgdump::format::custom::CustomReader;

pub use sink::{ParquetSink, ParquetSinkFactory, SinkOpts, SinkStats, TableSchema};

/// Stream one TABLE DATA entry into `sink`. Returns the row count written.
///
/// The sink is driven row-by-row via [`ParquetSink::append_row`]; the caller
/// is responsible for calling [`ParquetSink::close`] once they've decided the
/// output is complete (this function does not close, so the caller can add
/// its own bookkeeping — atomic rename, `.done` marker, etc. — before
/// finalising).
pub fn drive_table<R: Read + Seek, S: ParquetSink + ?Sized>(
    reader: &mut CustomReader<R>,
    dump_id: i32,
    sink: &mut S,
) -> Result<usize, DriveError> {
    let Some(stream) = reader
        .read_entry_stream(dump_id)
        .map_err(DriveError::Dump)?
    else {
        return Ok(0);
    };
    let mut lines = BufReader::new(stream);
    let mut line: Vec<u8> = Vec::new();
    let mut rows = 0usize;

    loop {
        line.clear();
        let n = lines.read_until(b'\n', &mut line).map_err(DriveError::Io)?;
        if n == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        if line == b"\\." {
            break;
        }
        let parsed = copy_text::parse_line(&line);
        // Convert to borrowed slices for the sink interface.
        let refs: Vec<Option<&[u8]>> = parsed.iter().map(|f| f.as_deref()).collect();
        sink.append_row(&refs).map_err(DriveError::Sink)?;
        rows += 1;
    }
    Ok(rows)
}

#[derive(Debug, thiserror::Error)]
pub enum DriveError {
    #[error("libpgdump error: {0}")]
    Dump(#[from] libpgdump::Error),
    #[error("io error while reading COPY stream: {0}")]
    Io(std::io::Error),
    #[error("sink error: {0}")]
    Sink(#[source] Box<dyn std::error::Error + Send + Sync>),
}
