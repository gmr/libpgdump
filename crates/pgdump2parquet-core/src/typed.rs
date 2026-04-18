//! Typed Arrow output.
//!
//! The pipeline used to hand sinks an all-`Utf8` `ColumnarBlock` and punt
//! typing downstream (Parquet `VARCHAR` + DuckDB `TRY_CAST`). We have the
//! DDL, so we type up-front: the driver builds an `arrow::Schema` from the
//! parsed `CREATE TABLE`, converts each block into a typed `RecordBatch`,
//! and sinks write typed Parquet directly.
//!
//! Conversion is per-column:
//!
//! * `Utf8` stays zero-copy — the `(values, offsets, validity)` buffers
//!   from the block pipeline are handed straight to `StringArray::try_new`.
//! * Typed columns parse each decoded cell. Parse failure → NULL, matching
//!   the prior `TRY_CAST` semantics: one bad value doesn't kill the table.
//!
//! Types implemented today: `Utf8`, `Int16/32/64`, `Float32/64`, `Boolean`,
//! `Date32`, `Timestamp(Microsecond, None)`, `Decimal128(p,s)`. Anything else
//! falls back to `Utf8` so no value is ever dropped — including PG arrays,
//! `bytea`, UUIDs, JSON/JSONB, and `timestamptz` (which pg_dump emits with
//! a trailing offset we don't want to eat yet).

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
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
/// pre-built schema. `Utf8` columns stay zero-copy; typed columns parse
/// per-cell, swallowing parse errors to NULL.
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
        .map(|(col, field)| build_typed_column(col, field.data_type(), n_rows))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(schema.clone(), arrays)
}

fn build_typed_column(
    col: ColumnBuffer,
    data_type: &DataType,
    n_rows: usize,
) -> Result<ArrayRef, ArrowError> {
    match data_type {
        DataType::Utf8 => build_utf8(col, n_rows),
        DataType::Int16 => build_with(col, n_rows, Int16Builder::with_capacity(n_rows), |b, s| {
            s.parse::<i16>().ok().map(|v| b.append_value(v))
        }),
        DataType::Int32 => build_with(col, n_rows, Int32Builder::with_capacity(n_rows), |b, s| {
            s.parse::<i32>().ok().map(|v| b.append_value(v))
        }),
        DataType::Int64 => build_with(col, n_rows, Int64Builder::with_capacity(n_rows), |b, s| {
            s.parse::<i64>().ok().map(|v| b.append_value(v))
        }),
        DataType::Float32 => build_with(col, n_rows, Float32Builder::with_capacity(n_rows), |b, s| {
            parse_f32(s).map(|v| b.append_value(v))
        }),
        DataType::Float64 => build_with(col, n_rows, Float64Builder::with_capacity(n_rows), |b, s| {
            parse_f64(s).map(|v| b.append_value(v))
        }),
        DataType::Boolean => build_with(col, n_rows, BooleanBuilder::with_capacity(n_rows), |b, s| {
            parse_bool(s).map(|v| b.append_value(v))
        }),
        DataType::Date32 => build_with(col, n_rows, Date32Builder::with_capacity(n_rows), |b, s| {
            parse_date(s).map(|v| b.append_value(v))
        }),
        DataType::Timestamp(TimeUnit::Microsecond, None) => build_with(
            col,
            n_rows,
            TimestampMicrosecondBuilder::with_capacity(n_rows),
            |b, s| parse_timestamp_micros(s).map(|v| b.append_value(v)),
        ),
        DataType::Decimal128(p, s) => {
            let p = *p;
            let s = *s;
            let mut builder = Decimal128Builder::with_capacity(n_rows)
                .with_precision_and_scale(p, s)?;
            iter_decoded(&col, n_rows, |cell| match cell {
                None => builder.append_null(),
                Some(bytes) => match std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|txt| parse_decimal_i128(txt, s))
                {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                },
            });
            Ok(Arc::new(builder.finish()))
        }
        // Fallback for types we don't decode yet: treat as Utf8. We already
        // have the bytes decoded from COPY-TEXT escapes, so this is a
        // lossless pass-through.
        _ => build_utf8(col, n_rows),
    }
}

fn build_utf8(col: ColumnBuffer, n_rows: usize) -> Result<ArrayRef, ArrowError> {
    // Zero-copy: the block pipeline already lays out Arrow `Utf8` buffers.
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
    let array = StringArray::try_new(offsets_buf, values_buf, nulls)?;
    Ok(Arc::new(array))
}

/// Walk the decoded bytes of every cell in `col`, invoking `f` with
/// `Some(bytes)` for non-NULL rows (bytes are the already-COPY-unescaped
/// values) and `None` for NULL rows.
fn iter_decoded<'a>(
    col: &'a ColumnBuffer,
    n_rows: usize,
    mut f: impl FnMut(Option<&'a [u8]>),
) {
    let validity = col.validity.as_deref();
    for r in 0..n_rows {
        let is_null = match validity {
            None => false,
            Some(bits) => bits[r / 8] & (1u8 << (r % 8)) == 0,
        };
        if is_null {
            f(None);
            continue;
        }
        let start = col.offsets[r] as usize;
        let end = col.offsets[r + 1] as usize;
        f(Some(&col.values[start..end]));
    }
}

/// Common shape for typed builders: parse each cell's bytes as UTF-8,
/// hand the `&str` to `push` which appends to the builder or returns
/// `None` to NULL. Builders are finalised with `finish()`.
fn build_with<B: arrow_array::builder::ArrayBuilder>(
    col: ColumnBuffer,
    n_rows: usize,
    mut builder: B,
    mut push: impl FnMut(&mut B, &str) -> Option<()>,
) -> Result<ArrayRef, ArrowError>
where
    B: NullAppend,
{
    iter_decoded(&col, n_rows, |cell| match cell {
        None => builder.append_null_opaque(),
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => {
                if push(&mut builder, s).is_none() {
                    builder.append_null_opaque();
                }
            }
            Err(_) => builder.append_null_opaque(),
        },
    });
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Shim so `build_with` can call `append_null` uniformly on every typed
/// builder (the real method is inherent, not part of `ArrayBuilder`).
trait NullAppend {
    fn append_null_opaque(&mut self);
}

macro_rules! impl_null_append {
    ($($t:ty),* $(,)?) => {
        $(impl NullAppend for $t {
            fn append_null_opaque(&mut self) { self.append_null(); }
        })*
    }
}

impl_null_append!(
    Int16Builder,
    Int32Builder,
    Int64Builder,
    Float32Builder,
    Float64Builder,
    BooleanBuilder,
    Date32Builder,
    TimestampMicrosecondBuilder,
);

// ---- tiny, targeted parsers (COPY TEXT emits a very narrow format) ----

fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "t" | "true" | "TRUE" => Some(true),
        "f" | "false" | "FALSE" => Some(false),
        _ => None,
    }
}

fn parse_f32(s: &str) -> Option<f32> {
    match s {
        "NaN" => Some(f32::NAN),
        "Infinity" => Some(f32::INFINITY),
        "-Infinity" => Some(f32::NEG_INFINITY),
        other => other.parse().ok(),
    }
}

fn parse_f64(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        other => other.parse().ok(),
    }
}

/// Days from UNIX epoch (1970-01-01) for a pg COPY-TEXT date `YYYY-MM-DD`.
fn parse_date(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    // Accept the canonical 10-byte ISO form first (99% of rows).
    if b.len() == 10 && b[4] == b'-' && b[7] == b'-' {
        let y: i32 = std::str::from_utf8(&b[0..4]).ok()?.parse().ok()?;
        let m: u32 = std::str::from_utf8(&b[5..7]).ok()?.parse().ok()?;
        let d: u32 = std::str::from_utf8(&b[8..10]).ok()?.parse().ok()?;
        return days_from_civil(y, m, d);
    }
    None
}

/// Microseconds since UNIX epoch for `YYYY-MM-DD HH:MM:SS[.fraction]`.
fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19 || b[4] != b'-' || b[7] != b'-' || b[10] != b' ' || b[13] != b':' || b[16] != b':'
    {
        return None;
    }
    let y: i32 = std::str::from_utf8(&b[0..4]).ok()?.parse().ok()?;
    let mo: u32 = std::str::from_utf8(&b[5..7]).ok()?.parse().ok()?;
    let d: u32 = std::str::from_utf8(&b[8..10]).ok()?.parse().ok()?;
    let hh: u32 = std::str::from_utf8(&b[11..13]).ok()?.parse().ok()?;
    let mm: u32 = std::str::from_utf8(&b[14..16]).ok()?.parse().ok()?;
    let ss: u32 = std::str::from_utf8(&b[17..19]).ok()?.parse().ok()?;
    let days = days_from_civil(y, mo, d)?;
    let base_us: i64 = i64::from(days) * 86_400_000_000
        + i64::from(hh) * 3_600_000_000
        + i64::from(mm) * 60_000_000
        + i64::from(ss) * 1_000_000;
    // Optional fractional seconds `.123`, `.123456`, etc. Reject anything
    // that isn't pure digits — we don't want to silently truncate a TZ
    // suffix that only timestamptz should carry.
    if b.len() == 19 {
        return Some(base_us);
    }
    if b[19] != b'.' {
        return None;
    }
    let frac = &b[20..];
    if frac.is_empty() || !frac.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Pad or truncate to exactly 6 digits (micros).
    let mut micros: i64 = 0;
    let mut i = 0;
    while i < 6 {
        micros *= 10;
        if i < frac.len() {
            micros += (frac[i] - b'0') as i64;
        }
        i += 1;
    }
    Some(base_us + micros)
}

/// Days since the Unix epoch (1970-01-01) for a proleptic Gregorian date.
/// Based on Howard Hinnant's date algorithms (public domain).
fn days_from_civil(y: i32, m: u32, d: u32) -> Option<i32> {
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe as i32 - 719_468;
    Some(days)
}

/// Parse `-123.45` into an i128 scaled by `scale`. Returns `None` if the
/// string has more fractional digits than `scale` or if anything is
/// non-numeric. Accepts an optional leading `+` or `-`.
fn parse_decimal_i128(s: &str, scale: i8) -> Option<i128> {
    if scale < 0 {
        return None;
    }
    let scale = scale as usize;
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let (neg, rest) = match bytes[0] {
        b'-' => (true, &bytes[1..]),
        b'+' => (false, &bytes[1..]),
        _ => (false, bytes),
    };
    if rest.is_empty() {
        return None;
    }

    let mut int_part: i128 = 0;
    let mut i = 0;
    while i < rest.len() && rest[i] != b'.' {
        let c = rest[i];
        if !c.is_ascii_digit() {
            return None;
        }
        int_part = int_part.checked_mul(10)?.checked_add((c - b'0') as i128)?;
        i += 1;
    }

    let mut frac_part: i128 = 0;
    let mut frac_len = 0usize;
    if i < rest.len() {
        // consume '.'
        i += 1;
        while i < rest.len() {
            let c = rest[i];
            if !c.is_ascii_digit() {
                return None;
            }
            if frac_len < scale {
                frac_part = frac_part.checked_mul(10)?.checked_add((c - b'0') as i128)?;
                frac_len += 1;
            } else if c != b'0' {
                // Reject lossy truncation: we'd be dropping a non-zero digit.
                return None;
            }
            i += 1;
        }
    }
    // Pad fractional to scale.
    while frac_len < scale {
        frac_part = frac_part.checked_mul(10)?;
        frac_len += 1;
    }
    let scale_factor: i128 = 10i128.checked_pow(scale as u32)?;
    let mut v = int_part.checked_mul(scale_factor)?.checked_add(frac_part)?;
    if neg {
        v = -v;
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(pg_to_arrow_type("numeric(10,2)"), DataType::Decimal128(10, 2));
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

    #[test]
    fn date_parses() {
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("1970-01-02"), Some(1));
        assert_eq!(parse_date("1969-12-31"), Some(-1));
        assert_eq!(parse_date("2020-02-29"), Some(18_321));
        assert_eq!(parse_date("2020-13-01"), None);
    }

    #[test]
    fn timestamp_parses() {
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00"), Some(0));
        assert_eq!(
            parse_timestamp_micros("1970-01-01 00:00:00.000001"),
            Some(1)
        );
        assert_eq!(
            parse_timestamp_micros("1970-01-01 00:00:01.5"),
            Some(1_500_000)
        );
        // TZ suffix on a timestamp-without-tz column is rejected, not
        // silently truncated — we'd rather NULL than lie.
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00+00"), None);
        assert_eq!(parse_timestamp_micros("not-a-timestamp"), None);
    }

    #[test]
    fn decimal_parses() {
        assert_eq!(parse_decimal_i128("123.45", 2), Some(12345));
        assert_eq!(parse_decimal_i128("-123.45", 2), Some(-12345));
        assert_eq!(parse_decimal_i128("0", 2), Some(0));
        assert_eq!(parse_decimal_i128("0.1", 2), Some(10));
        // Over-scale trailing zeros are fine.
        assert_eq!(parse_decimal_i128("1.1000", 2), Some(110));
        // Over-scale non-zero digit is a parse failure (no silent truncation).
        assert_eq!(parse_decimal_i128("1.125", 2), None);
        // Non-digit junk.
        assert_eq!(parse_decimal_i128("abc", 2), None);
        assert_eq!(parse_decimal_i128("", 2), None);
    }

    #[test]
    fn bool_parses() {
        assert_eq!(parse_bool("t"), Some(true));
        assert_eq!(parse_bool("f"), Some(false));
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("x"), None);
    }
}
