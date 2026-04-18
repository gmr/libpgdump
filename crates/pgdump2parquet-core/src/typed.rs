//! Typed Arrow output.
//!
//! The pipeline used to hand sinks an all-`Utf8` `ColumnarBlock` and punt
//! typing downstream (Parquet `VARCHAR` + DuckDB `TRY_CAST`). We have the
//! DDL, so we type up-front: the driver builds an `arrow::Schema` from the
//! parsed `CREATE TABLE`, converts each block into a typed `RecordBatch`,
//! and sinks write typed Parquet directly.
//!
//! Per-column conversion is **vectorised via `arrow_cast`**:
//!
//! 1. Every column is first materialised as a zero-copy `StringArray` —
//!    the `(values, offsets, validity)` buffers from the block pipeline
//!    already match Arrow's `Utf8` layout, so we `Buffer::from_vec` and
//!    hand them to `StringArray::new_unchecked`.
//! 2. `arrow_cast::cast_with_options` then parses the string column into
//!    the target type in one shot, with `safe: true` so unparseable values
//!    become `NULL` instead of erroring (matches the prior `TRY_CAST`
//!    semantics — one bad value doesn't kill the table).
//!
//! Supported target types today: `Utf8`, `Int16/32/64`, `Float32/64`,
//! `Boolean`, `Date32`, `Timestamp(Microsecond, None)`,
//! `Decimal128(p,s)`. Anything else falls back to `Utf8` so no value is
//! ever dropped — including PG arrays, `bytea`, UUIDs, JSON/JSONB, and
//! `timestamptz` (whose offset we don't want to eat yet).

use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::block::{ColumnBuffer, ColumnarBlock};
use crate::ddl::ColumnDef;

/// Build an Arrow `Schema` from parsed pg_dump column definitions.
pub fn build_arrow_schema(columns: &[ColumnDef]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, pg_to_arrow_type(&c.pg_type), true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Map a pg_dump-style type name to the `DataType` we emit in the
/// resulting Parquet. Unknown or complex types stay as `Utf8` so the value
/// is never lost (arrays, `bytea`, `uuid`, `json`/`jsonb`, `timestamptz`).
pub fn pg_to_arrow_type(pg_type: &str) -> DataType {
    let t = pg_type.trim();
    if t.ends_with("[]") || t.starts_with("ARRAY") || t.contains(" ARRAY") {
        return DataType::Utf8;
    }

    let lower = t.to_ascii_lowercase();
    let (base, paren) = match lower.find('(') {
        Some(i) => {
            let close = lower[i..]
                .find(')')
                .map(|j| i + j + 1)
                .unwrap_or(lower.len());
            (lower[..i].trim(), Some(&lower[i + 1..close - 1]))
        }
        None => (lower.as_str(), None),
    };

    match base {
        "smallint" | "int2" => DataType::Int16,
        "integer" | "int" | "int4" => DataType::Int32,
        "bigint" | "int8" => DataType::Int64,
        "real" | "float4" => DataType::Float32,
        "double precision" | "float8" => DataType::Float64,
        "numeric" | "decimal" => decimal_type(paren),
        "boolean" | "bool" => DataType::Boolean,
        "date" => DataType::Date32,
        // Timestamp *without* time zone — pg_dump emits `YYYY-MM-DD HH:MM:SS[.frac]`.
        // arrow_cast's parser handles the space-separated form natively.
        "timestamp" | "timestamp without time zone" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        // Everything else — text/varchar/char, json/jsonb, uuid, bytea,
        // timestamptz, time, interval, network types, geometry, etc. —
        // stays as `Utf8`. Preserves the value losslessly.
        _ => DataType::Utf8,
    }
}

fn decimal_type(paren: Option<&str>) -> DataType {
    // pg's NUMERIC default precision/scale is arbitrary; Arrow caps
    // `Decimal128` at precision 38. If the DDL declares something larger,
    // we'd lose data on overflow — fall back to Utf8 in that case.
    let (p, s) = match paren {
        Some(p) => {
            let mut parts = p.split(',').map(str::trim);
            let prec: Option<u8> = parts.next().and_then(|v| v.parse().ok());
            let scale: i8 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            match prec {
                Some(p) if p <= 38 => (p, scale),
                _ => return DataType::Utf8,
            }
        }
        None => (38, 10),
    };
    DataType::Decimal128(p, s)
}

/// Convert a `ColumnarBlock` into a typed Arrow `RecordBatch` against a
/// pre-built schema. Each column goes `ColumnBuffer -> StringArray`
/// (zero-copy) `-> arrow_cast` to the target type. Parse failures become
/// NULLs.
pub fn block_to_record_batch(
    block: ColumnarBlock,
    schema: &SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    if block.columns.len() != schema.fields().len() {
        return Err(ArrowError::SchemaError(format!(
            "column count mismatch: block has {}, schema has {}",
            block.columns.len(),
            schema.fields().len()
        )));
    }
    let n_rows = block.n_rows;
    let arrays: Vec<ArrayRef> = block
        .columns
        .into_iter()
        .zip(schema.fields().iter())
        .map(|(col, field)| cast_column(col, field.data_type(), n_rows))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(schema.clone(), arrays)
}

fn cast_column(
    col: ColumnBuffer,
    data_type: &DataType,
    n_rows: usize,
) -> Result<ArrayRef, ArrowError> {
    let utf8: ArrayRef = Arc::new(build_utf8(col, n_rows)?);
    if matches!(data_type, DataType::Utf8) {
        return Ok(utf8);
    }
    if matches!(data_type, DataType::Boolean) {
        // arrow_cast's Utf8 -> Boolean parses "true"/"false" only, but
        // pg_dump emits `t`/`f`. Rewrite here rather than ship two values
        // through just to have arrow_cast reject one of them.
        return cast_boolean_tf(&utf8, n_rows);
    }
    // safe=true: parse failure -> NULL. Matches the prior TRY_CAST policy.
    let opts = CastOptions {
        safe: true,
        ..Default::default()
    };
    cast_with_options(&utf8, data_type, &opts)
}

/// pg `COPY TEXT` encodes booleans as `t` / `f`. arrow_cast wants
/// `true` / `false`, so do the conversion inline; NULLs pass through.
fn cast_boolean_tf(utf8: &ArrayRef, n_rows: usize) -> Result<ArrayRef, ArrowError> {
    let sa = utf8
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::CastError("expected StringArray".into()))?;
    let mut b = BooleanBuilder::with_capacity(n_rows);
    for i in 0..n_rows {
        if sa.is_null(i) {
            b.append_null();
            continue;
        }
        match sa.value(i) {
            "t" | "true" | "TRUE" => b.append_value(true),
            "f" | "false" | "FALSE" => b.append_value(false),
            _ => b.append_null(),
        }
    }
    Ok(Arc::new(b.finish()))
}

fn build_utf8(col: ColumnBuffer, n_rows: usize) -> Result<StringArray, ArrowError> {
    // Zero-copy: the block pipeline already lays out Arrow `Utf8` buffers.
    // We rely on the COPY parser emitting valid UTF-8 in the common case
    // and skip validation via `new_unchecked`.
    let ColumnBuffer {
        values,
        offsets,
        validity,
    } = col;
    if offsets.len() != n_rows + 1 {
        return Err(ArrowError::SchemaError(format!(
            "offsets length {} != n_rows+1 ({})",
            offsets.len(),
            n_rows + 1
        )));
    }
    let values_buf = Buffer::from_vec(values);
    let offsets_scalar = ScalarBuffer::from(offsets);
    let offsets_buf = OffsetBuffer::new(offsets_scalar);
    let nulls = validity.map(|bytes| {
        let buf = Buffer::from_vec(bytes);
        NullBuffer::new(arrow_buffer::BooleanBuffer::new(buf, 0, n_rows))
    });
    // SAFETY: UTF-8 invariant held by the COPY-TEXT producer + our decode
    // pass. Skipping validation is intentional (per-value scan is the
    // slowest part of Utf8 construction).
    Ok(unsafe { StringArray::new_unchecked(offsets_buf, values_buf, nulls) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Date32Array, Decimal128Array, Int32Array};

    #[test]
    fn type_mapping() {
        assert_eq!(pg_to_arrow_type("integer"), DataType::Int32);
        assert_eq!(pg_to_arrow_type("bigint"), DataType::Int64);
        assert_eq!(pg_to_arrow_type("smallint"), DataType::Int16);
        assert_eq!(pg_to_arrow_type("double precision"), DataType::Float64);
        assert_eq!(pg_to_arrow_type("real"), DataType::Float32);
        assert_eq!(pg_to_arrow_type("boolean"), DataType::Boolean);
        assert_eq!(pg_to_arrow_type("date"), DataType::Date32);
        assert_eq!(
            pg_to_arrow_type("timestamp without time zone"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            pg_to_arrow_type("timestamp with time zone"),
            DataType::Utf8,
            "timestamptz stays Utf8 — pg_dump's offset form isn't decoded yet"
        );
        assert_eq!(
            pg_to_arrow_type("numeric(10,2)"),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(pg_to_arrow_type("numeric"), DataType::Decimal128(38, 10));
        assert_eq!(
            pg_to_arrow_type("numeric(60,10)"),
            DataType::Utf8,
            "precision > 38 overflows Decimal128 — fall back to Utf8"
        );
        assert_eq!(pg_to_arrow_type("text"), DataType::Utf8);
        assert_eq!(pg_to_arrow_type("character varying(64)"), DataType::Utf8);
        assert_eq!(pg_to_arrow_type("jsonb"), DataType::Utf8);
        assert_eq!(pg_to_arrow_type("text[]"), DataType::Utf8);
        assert_eq!(pg_to_arrow_type("uuid"), DataType::Utf8);
    }

    /// Build a `ColumnarBlock` from a column of strings (one column) so we
    /// can exercise the arrow_cast path end-to-end.
    fn single_col_block(values: &[Option<&str>]) -> ColumnarBlock {
        let n_rows = values.len();
        let mut buf = Vec::new();
        let mut offsets = Vec::with_capacity(n_rows + 1);
        offsets.push(0i32);
        let mut validity_bytes = vec![0u8; n_rows.div_ceil(8)];
        for (i, v) in values.iter().enumerate() {
            match v {
                Some(s) => {
                    buf.extend_from_slice(s.as_bytes());
                    validity_bytes[i / 8] |= 1 << (i % 8);
                }
                None => {}
            }
            offsets.push(buf.len() as i32);
        }
        let has_nulls = values.iter().any(|v| v.is_none());
        ColumnarBlock {
            n_rows,
            columns: vec![ColumnBuffer {
                values: buf,
                offsets,
                validity: has_nulls.then_some(validity_bytes),
            }],
        }
    }

    fn one_col_schema(dt: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("c", dt, true)]))
    }

    #[test]
    fn cast_int32() {
        let b = single_col_block(&[Some("1"), Some("-42"), None, Some("not-a-number")]);
        let schema = one_col_schema(DataType::Int32);
        let batch = block_to_record_batch(b, &schema).unwrap();
        let a = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.value(0), 1);
        assert_eq!(a.value(1), -42);
        assert!(a.is_null(2));
        assert!(a.is_null(3), "unparseable -> NULL under safe=true cast");
    }

    #[test]
    fn cast_boolean_tf() {
        let b = single_col_block(&[Some("t"), Some("f"), Some("true"), None, Some("?")]);
        let schema = one_col_schema(DataType::Boolean);
        let batch = block_to_record_batch(b, &schema).unwrap();
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(a.value(0));
        assert!(!a.value(1));
        assert!(a.value(2));
        assert!(a.is_null(3));
        assert!(a.is_null(4));
    }

    #[test]
    fn cast_date32() {
        let b = single_col_block(&[Some("1970-01-01"), Some("2020-02-29"), None]);
        let schema = one_col_schema(DataType::Date32);
        let batch = block_to_record_batch(b, &schema).unwrap();
        let a = batch.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(a.value(0), 0);
        assert_eq!(a.value(1), 18_321);
        assert!(a.is_null(2));
    }

    #[test]
    fn cast_decimal128() {
        // Normal cases only — pg_dump emits exactly scale-many digits for
        // typed numeric columns. Lossy-input behaviour (scale > target) is
        // arrow_cast's policy and not our contract to pin.
        let b = single_col_block(&[Some("123.45"), Some("-12.30"), None, Some("0.00")]);
        let schema = one_col_schema(DataType::Decimal128(10, 2));
        let batch = block_to_record_batch(b, &schema).unwrap();
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(a.value(0), 12345);
        assert_eq!(a.value(1), -1230);
        assert!(a.is_null(2));
        assert_eq!(a.value(3), 0);
    }

    #[test]
    fn utf8_passthrough_is_zero_copy_shaped() {
        // Sanity: a Utf8 column comes out intact, NULLs preserved.
        let b = single_col_block(&[Some("hello"), None, Some("world")]);
        let schema = one_col_schema(DataType::Utf8);
        let batch = block_to_record_batch(b, &schema).unwrap();
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "hello");
        assert!(a.is_null(1));
        assert_eq!(a.value(2), "world");
    }
}
