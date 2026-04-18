//! The sink abstraction backends implement.
//!
//! Each backend (arrow-rs, DuckDB, …) supplies:
//!
//! * A [`ParquetSinkFactory`] that knows how to open a fresh sink per table
//!   given the table's schema and caller-supplied options.
//! * A [`ParquetSink`] that accepts typed [`arrow_array::RecordBatch`]es and,
//!   on close, finalises the Parquet file and returns statistics.
//!
//! The sink interface is **batch-at-a-time**: the driver parses a block of
//! rows out of the COPY-TEXT stream, converts it to a typed `RecordBatch`
//! using the Arrow schema derived from the parsed DDL
//! ([`crate::typed::build_arrow_schema`]), and hands that batch to the sink.
//! Both sinks (arrow-rs, DuckDB) consume the same batch — the DuckDB sink
//! calls `Appender::append_record_batch`, the arrow sink calls
//! `ArrowWriter::write`.

use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};

use crate::ddl::ColumnDef;
use crate::typed;

/// A parsed table schema — exactly what the driver needs to hand to a sink.
/// Carries both the pg-side `ColumnDef`s (still useful for backend-specific
/// typed DDL, e.g. DuckDB's staging `CREATE TABLE`) and the Arrow schema
/// derived from them.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub arrow_schema: SchemaRef,
}

impl TableSchema {
    /// Build a `TableSchema` from the parsed DDL column list. The Arrow
    /// schema is computed eagerly so every sink sees the same types.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        let arrow_schema = typed::build_arrow_schema(&columns);
        Self {
            namespace: namespace.into(),
            name: name.into(),
            columns,
            arrow_schema,
        }
    }

    /// Build from a pre-computed Arrow schema (used when the caller has
    /// already materialised it or wants to override the default mapping).
    pub fn with_arrow_schema(
        namespace: impl Into<String>,
        name: impl Into<String>,
        columns: Vec<ColumnDef>,
        arrow_schema: SchemaRef,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            columns,
            arrow_schema,
        }
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }

    /// Convenience: number of columns in the Arrow schema.
    pub fn n_cols(&self) -> usize {
        self.arrow_schema.fields().len()
    }
}

// Convenience constructor for tests / callers that don't have a `SchemaRef` yet.
impl From<(String, String, Vec<ColumnDef>)> for TableSchema {
    fn from(t: (String, String, Vec<ColumnDef>)) -> Self {
        let arrow = Arc::new(Schema::new(Vec::<arrow_schema::Field>::new()));
        Self::with_arrow_schema(t.0, t.1, t.2, arrow)
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
pub trait ParquetSinkFactory: Send + Sync {
    fn open(&self, out: &Path, schema: &TableSchema) -> Result<Box<dyn ParquetSink>, SinkError>;
}

/// A per-table sink. One instance per output file.
pub trait ParquetSink: Send {
    /// Append a typed Arrow `RecordBatch` whose schema matches the one
    /// passed to [`ParquetSinkFactory::open`]. The driver builds these
    /// from the block pipeline via [`crate::typed::block_to_record_batch`].
    fn append_batch(&mut self, batch: RecordBatch) -> Result<(), SinkError>;

    /// Flush any buffered rows and finalise the output file.
    fn close(self: Box<Self>) -> Result<SinkStats, SinkError>;
}
