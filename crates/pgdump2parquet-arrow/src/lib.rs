//! Arrow-rs + parquet-crate backend for `pgdump2parquet`.
//!
//! Takes the typed `RecordBatch` the core driver produces (schema derived
//! from the parsed DDL via `pgdump2parquet_core::typed::build_arrow_schema`)
//! and hands it straight to `ArrowWriter::write`. No Utf8 rewrite, no
//! downstream `TRY_CAST` needed — the Parquet file is typed.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use pgdump2parquet_core::sink::{
    ParquetSink, ParquetSinkFactory, SinkError, SinkOpts, SinkStats, TableSchema,
};

/// Factory that produces arrow-rs-backed Parquet sinks.
#[derive(Debug, Clone)]
pub struct ArrowFactory {
    pub opts: SinkOpts,
}

impl ArrowFactory {
    pub fn new(opts: SinkOpts) -> Self {
        Self { opts }
    }
}

impl ParquetSinkFactory for ArrowFactory {
    fn open(&self, out: &Path, schema: &TableSchema) -> Result<Box<dyn ParquetSink>, SinkError> {
        let arrow_schema = schema.arrow_schema.clone();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(self.opts.zstd_level).map_err(boxed)?,
            ))
            .set_max_row_group_size(self.opts.row_group_rows)
            // Page-index truncation default is 64 bytes per parquet-rs.
            // We leave it on: typical USAspending-style string values fit
            // inside that, and disabling truncation explodes the index
            // metadata payload (the `None` variant made a typed-output
            // full-run regress from ~80s to 6 min).
            .build();
        let file = File::create(out).map_err(boxed)?;
        let writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).map_err(boxed)?;
        Ok(Box::new(ArrowSink {
            writer,
            total_rows: 0,
            _out: out.to_path_buf(),
        }))
    }
}

struct ArrowSink {
    writer: ArrowWriter<File>,
    total_rows: usize,
    _out: PathBuf,
}

impl ParquetSink for ArrowSink {
    fn append_batch(&mut self, batch: RecordBatch) -> Result<(), SinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let n = batch.num_rows();
        self.writer.write(&batch).map_err(boxed)?;
        self.total_rows += n;
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<SinkStats, SinkError> {
        self.writer.close().map_err(boxed)?;
        Ok(SinkStats {
            rows_written: self.total_rows,
        })
    }
}

fn boxed<E: std::error::Error + Send + Sync + 'static>(e: E) -> SinkError {
    Box::new(e)
}
