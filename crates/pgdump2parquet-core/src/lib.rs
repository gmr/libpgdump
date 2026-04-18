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
pub mod directory;
pub mod sink;
pub mod typed;

use std::io::{Read, Seek};

use arrow_schema::SchemaRef;
use libpgdump::format::custom::CustomReader;

pub use block::{BlockFrame, BlockReader, ColumnBuffer, ColumnarBlock, FieldRanges};
pub use directory::{DirectoryInput, TocEntry};
pub use sink::{ParquetSink, ParquetSinkFactory, SinkOpts, SinkStats, TableSchema};

/// 4MB block target — big enough that parquet writers don't see excessive
/// tiny RecordBatches, small enough that peak arena stays a few MB per
/// worker.
pub const DEFAULT_BLOCK_TARGET: usize = 4 * 1024 * 1024;

/// Stream one TABLE DATA entry from a custom-format (`-Fc`) dump into
/// `sink`. Returns the row count written.
///
/// For directory-format (`-Fd`) dumps the caller has a raw decompressor
/// stream per table and should call [`drive_stream`] directly.
///
/// The sink is driven **block-at-a-time**: a [`BlockReader`] frames rows out
/// of the decompressed stream into a reusable arena, fields are split with
/// vectorised `memchr`, and per-column buffers (Arrow-shaped) are handed to
/// [`ParquetSink::append_block`]. Columns whose fields contain no backslash
/// escapes skip the decode pass entirely — their values are copied straight
/// from the arena. The caller is still responsible for calling
/// [`ParquetSink::close`] after the final block.
///
/// `arrow_schema` drives both the block-to-batch typed conversion and the
/// sink's output schema — callers build it once from the parsed DDL via
/// [`typed::build_arrow_schema`] and thread the same `SchemaRef` through
/// the sink factory and the driver. Rows with fewer fields than the
/// schema declares are padded with NULLs; extras are discarded (pg_dump
/// output is rectangular in practice, but the driver is defensive).
pub fn drive_table<R: Read + Seek, S: ParquetSink + ?Sized>(
    reader: &mut CustomReader<R>,
    dump_id: i32,
    arrow_schema: &SchemaRef,
    sink: &mut S,
) -> Result<usize, DriveError> {
    let Some(stream) = reader
        .read_entry_stream(dump_id)
        .map_err(DriveError::Dump)?
    else {
        return Ok(0);
    };
    drive_stream(stream, arrow_schema, sink)
}

/// Drive the block pipeline from an arbitrary `Read` source (the same
/// decompressed COPY-TEXT byte stream that [`drive_table`] produces
/// internally). This is the entrypoint for the directory-format (`-Fd`)
/// path, where the caller opens one gzip (or lz4/zstd) file per table and
/// feeds it in directly — libpgdump isn't involved beyond TOC parsing.
pub fn drive_stream<R: Read, S: ParquetSink + ?Sized>(
    stream: R,
    arrow_schema: &SchemaRef,
    sink: &mut S,
) -> Result<usize, DriveError> {
    let n_cols = arrow_schema.fields().len();
    let mut br = BlockReader::new(stream, DEFAULT_BLOCK_TARGET);
    let mut ranges = FieldRanges::new();
    let mut rows = 0usize;

    while let Some(frame) = br.next_block().map_err(DriveError::Io)? {
        let eod = frame.eod;
        if frame.row_offsets.len() > 1 {
            ranges.fill(&frame, n_cols);
            rows += ranges.n_rows;
            let block = ColumnarBlock::build(&frame, &ranges);
            // Free the frame's borrow before handing ownership of the
            // block off; we're done reading from the arena for this batch.
            let _ = frame;
            let batch = typed::block_to_record_batch(block, arrow_schema)
                .map_err(DriveError::Arrow)?;
            sink.append_batch(batch).map_err(DriveError::Sink)?;
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
    #[error("arrow error while building typed batch: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("sink error: {0}")]
    Sink(#[source] Box<dyn std::error::Error + Send + Sync>),
}
