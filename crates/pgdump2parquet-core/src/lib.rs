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

pub mod block;
pub mod copy_text;
pub mod ddl;
pub mod sink;

use std::io::{Read, Seek};

use libpgdump::format::custom::CustomReader;

pub use block::{BlockFrame, BlockReader, ColumnBuffer, ColumnarBlock, FieldRanges};
pub use sink::{ParquetSink, ParquetSinkFactory, SinkOpts, SinkStats, TableSchema};

/// Stream one TABLE DATA entry into `sink`. Returns the row count written.
///
/// The sink is driven **block-at-a-time**: a [`BlockReader`] frames rows out
/// of the decompressed stream into a reusable arena, fields are split with
/// vectorised `memchr`, and per-column buffers (Arrow-shaped) are handed to
/// [`ParquetSink::append_block`]. Columns whose fields contain no backslash
/// escapes skip the decode pass entirely — their values are copied straight
/// from the arena. The caller is still responsible for calling
/// [`ParquetSink::close`] after the final block.
///
/// `n_cols` is the declared column count for the table. Rows with fewer
/// fields are padded with NULLs; rows with more have their extras discarded
/// (pg_dump output is rectangular in practice, but the driver is defensive).
pub fn drive_table<R: Read + Seek, S: ParquetSink + ?Sized>(
    reader: &mut CustomReader<R>,
    dump_id: i32,
    n_cols: usize,
    sink: &mut S,
) -> Result<usize, DriveError> {
    let Some(stream) = reader
        .read_entry_stream(dump_id)
        .map_err(DriveError::Dump)?
    else {
        return Ok(0);
    };

    // 4MB blocks by default — big enough that parquet writers don't see
    // excessive tiny RecordBatches, small enough that peak arena stays a
    // few MB per worker.
    const BLOCK_TARGET: usize = 4 * 1024 * 1024;
    let mut br = BlockReader::new(stream, BLOCK_TARGET);
    let mut ranges = FieldRanges::new();
    let mut rows = 0usize;

    while let Some(frame) = br.next_block().map_err(DriveError::Io)? {
        let eod = frame.eod;
        if frame.row_offsets.len() > 1 {
            ranges.fill(&frame, n_cols);
            rows += ranges.n_rows;
            let block = ColumnarBlock::build(&frame, &ranges);
            // `block` owns its buffers; the `frame` borrow can be released
            // here so the sink isn't limited by it.
            let _ = frame;
            sink.append_block(block).map_err(DriveError::Sink)?;
        }
        if eod {
            break;
        }
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
