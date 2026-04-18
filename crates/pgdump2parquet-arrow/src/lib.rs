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

use arrow_array::{RecordBatch, StringArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use pgdump2parquet_core::block::{ColumnBuffer, ColumnarBlock};
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
            ncols: schema.columns.len(),
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
    ncols: usize,
    total_rows: usize,
    _out: PathBuf,
}

impl ParquetSink for ArrowSink {
    fn append_block(&mut self, block: ColumnarBlock) -> Result<(), SinkError> {
        if block.n_rows == 0 {
            return Ok(());
        }
        if block.columns.len() != self.ncols {
            return Err(boxed(std::io::Error::other(format!(
                "column count mismatch: block has {}, schema has {}",
                block.columns.len(),
                self.ncols
            ))));
        }

        let n_rows = block.n_rows;
        let arrays: Vec<Arc<dyn arrow_array::Array>> = block
            .columns
            .into_iter()
            .map(|col| column_to_string_array(col, n_rows))
            .collect::<Result<_, _>>()?;
        let batch = RecordBatch::try_new(self.schema.clone(), arrays).map_err(boxed)?;
        self.writer.write(&batch).map_err(boxed)?;
        self.total_rows += n_rows;
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<SinkStats, SinkError> {
        self.writer.close().map_err(boxed)?;
        Ok(SinkStats {
            rows_written: self.total_rows,
        })
    }
}

/// Convert a [`ColumnBuffer`] (which is already in Arrow's `Utf8` physical
/// layout) into a `StringArray`. We move the `Vec<u8>` buffers directly into
/// Arrow's `Buffer` via `Buffer::from_vec` — **no memcpy**. This is the key
/// perf property of the block pipeline: the bytes decoded by the core crate
/// are the same bytes Arrow serialises to Parquet.
fn column_to_string_array(
    col: ColumnBuffer,
    n_rows: usize,
) -> Result<Arc<dyn arrow_array::Array>, SinkError> {
    debug_assert_eq!(col.offsets.len(), n_rows + 1);

    let values_buf = Buffer::from_vec(col.values);
    let offsets_scalar: ScalarBuffer<i32> = ScalarBuffer::from(col.offsets);
    let offsets = OffsetBuffer::new(offsets_scalar);

    let nulls = col.validity.map(|bits| {
        NullBuffer::new(arrow_buffer::BooleanBuffer::new(
            Buffer::from_vec(bits),
            0,
            n_rows,
        ))
    });

    // SAFETY: we rely on the COPY parser emitting valid UTF-8 in the common
    // case. If a dump contains non-UTF-8 in a text field (rare, and strongly
    // discouraged by pg conventions), ArrowWriter will reject the batch on
    // encode. Using `new_unchecked` skips the per-value UTF-8 validation
    // pass — that's the slow bit we're trying to avoid. The Parquet writer
    // itself will still error out cleanly on invalid UTF-8 when encoding.
    let array = unsafe { StringArray::new_unchecked(offsets, values_buf, nulls) };
    Ok(Arc::new(array))
}

fn boxed<E: std::error::Error + Send + Sync + 'static>(e: E) -> SinkError {
    Box::new(e)
}
