//! Arrow-rs + parquet-crate backend for `pgdump2parquet`.
//!
//! All columns are written as Parquet VARCHAR (Arrow `Utf8`). This keeps the
//! backend small and correct against every pg type — downstream tools like
//! DuckDB can `TRY_CAST` to typed columns cheaply in a follow-up query. The
//! DuckDB backend (`pgdump2parquet-duckdb`) does that cast at write time
//! instead; pick whichever fits.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{RecordBatch, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema};
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
        let arrow_schema = build_schema(schema);
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(self.opts.zstd_level).map_err(boxed)?,
            ))
            .set_max_row_group_size(self.opts.row_group_rows)
            .build();
        let file = File::create(out).map_err(boxed)?;
        let writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).map_err(boxed)?;
        Ok(Box::new(ArrowSink {
            writer,
            schema: arrow_schema,
            builders: schema.columns.iter().map(|_| StringBuilder::new()).collect(),
            ncols: schema.columns.len(),
            batch_rows: self.opts.batch_rows,
            pending: 0,
            total_rows: 0,
            _out: out.to_path_buf(),
        }))
    }
}

fn build_schema(schema: &TableSchema) -> Arc<Schema> {
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|c| Field::new(&c.name, DataType::Utf8, true))
        .collect();
    Arc::new(Schema::new(fields))
}

struct ArrowSink {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    builders: Vec<StringBuilder>,
    ncols: usize,
    batch_rows: usize,
    pending: usize,
    total_rows: usize,
    _out: PathBuf,
}

impl ParquetSink for ArrowSink {
    fn append_row(&mut self, fields: &[Option<&[u8]>]) -> Result<(), SinkError> {
        for i in 0..self.ncols {
            match fields.get(i).copied().flatten() {
                Some(bytes) => {
                    let s = match std::str::from_utf8(bytes) {
                        Ok(s) => std::borrow::Cow::Borrowed(s),
                        Err(_) => String::from_utf8_lossy(bytes),
                    };
                    self.builders[i].append_value(s.as_ref());
                }
                None => self.builders[i].append_null(),
            }
        }
        self.pending += 1;
        self.total_rows += 1;
        if self.pending >= self.batch_rows {
            self.flush_internal()?;
        }
        Ok(())
    }

    fn close(mut self: Box<Self>) -> Result<SinkStats, SinkError> {
        if self.pending > 0 {
            self.flush_internal()?;
        }
        self.writer.close().map_err(boxed)?;
        Ok(SinkStats {
            rows_written: self.total_rows,
        })
    }
}

impl ArrowSink {
    fn flush_internal(&mut self) -> Result<(), SinkError> {
        let arrays: Vec<Arc<dyn arrow_array::Array>> = self
            .builders
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as Arc<dyn arrow_array::Array>)
            .collect();
        let batch = RecordBatch::try_new(self.schema.clone(), arrays).map_err(boxed)?;
        self.writer.write(&batch).map_err(boxed)?;
        self.pending = 0;
        Ok(())
    }
}

fn boxed<E: std::error::Error + Send + Sync + 'static>(e: E) -> SinkError {
    Box::new(e)
}
