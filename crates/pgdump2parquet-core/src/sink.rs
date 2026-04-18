//! The sink abstraction backends implement.
//!
//! Each backend (arrow-rs, DuckDB, …) supplies:
//!
//! * A [`ParquetSinkFactory`] that knows how to open a fresh sink per table
//!   given the table's schema and caller-supplied options.
//! * A [`ParquetSink`] that accepts parsed blocks and, on close, finalises
//!   the Parquet file and returns statistics.
//!
//! The sink interface is **block-at-a-time**: the driver hands the sink a
//! [`ColumnarBlock`][crate::block::ColumnarBlock] — per-column Arrow-shaped
//! `(values, offsets, validity)` buffers — and the sink decides how to
//! incorporate that into its output. Arrow backends can consume the buffers
//! essentially zero-copy; row-oriented backends (DuckDB Appender) walk the
//! offsets without going back to per-row allocations.

use std::path::Path;

use crate::block::ColumnarBlock;
use crate::ddl::ColumnDef;

/// A parsed table schema — exactly what the driver needs to hand to a sink.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}

/// Knobs that apply to every sink.
#[derive(Debug, Clone)]
pub struct SinkOpts {
    /// Rows per internal flush (Arrow `RecordBatch` / DuckDB Appender
    /// checkpoint). Backends may ignore.
    pub batch_rows: usize,
    /// Rows per Parquet row group.
    pub row_group_rows: usize,
    /// zstd level (1–22). Parquet-side compression.
    pub zstd_level: i32,
}

impl Default for SinkOpts {
    fn default() -> Self {
        Self {
            batch_rows: 65_536,
            row_group_rows: 1_000_000,
            zstd_level: 3,
        }
    }
}

/// Statistics returned by a sink when it closes.
#[derive(Debug, Clone, Copy, Default)]
pub struct SinkStats {
    pub rows_written: usize,
}

/// Boxed sink errors so backends can return their own error types without
/// this crate having to name them.
pub type SinkError = Box<dyn std::error::Error + Send + Sync>;

/// Factory that produces one sink per table.
///
/// The factory typically holds the user-chosen [`SinkOpts`] and whatever
/// engine handle the backend needs (a DuckDB connection pool, for example).
pub trait ParquetSinkFactory: Send + Sync {
    fn open(&self, out: &Path, schema: &TableSchema) -> Result<Box<dyn ParquetSink>, SinkError>;
}

/// A per-table sink. One instance per output Parquet file.
pub trait ParquetSink: Send {
    /// Append a block of rows. Ownership is moved into the sink so it can
    /// forward the underlying `Vec<u8>` buffers straight into the target
    /// storage (Arrow `Buffer`s etc.) without an intermediate copy.
    /// Column order matches the schema passed to [`ParquetSinkFactory::open`].
    /// For each column, `values[offsets[i]..offsets[i + 1]]` is row `i`'s
    /// bytes; row `i` is NULL when `validity` is present and bit `i` is clear.
    fn append_block(&mut self, block: ColumnarBlock) -> Result<(), SinkError>;

    /// Flush any buffered rows and finalise the Parquet file.
    fn close(self: Box<Self>) -> Result<SinkStats, SinkError>;
}
