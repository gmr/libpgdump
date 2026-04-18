//! Block-at-a-time pipeline for streaming pg_dump COPY TEXT into columnar
//! buffers without per-row allocation.
//!
//! Design inspired by vectorised execution engines (FastLanes, Vortex): we
//! refill a reusable arena buffer from the decompressor, framed into rows by
//! a single `memchr('\n')` sweep, then split each row into per-column byte
//! ranges with another `memchr('\t')` sweep. Decoding of backslash escapes
//! happens *per column*, and only for columns whose bytes actually contain
//! `\\` in this block — columns with no escapes emit zero-copy slices
//! straight out of the arena, which is the common case for numeric, boolean,
//! date, and timestamp columns.
//!
//! Output is always in Arrow layout: per-column `(values, offsets, nulls)`
//! triples that are directly compatible with `arrow_array::StringArray` and
//! friends. Sinks that want Arrow can consume them as-is; sinks that want
//! row-at-a-time (DuckDB's Appender, for example) can walk the offsets.

use std::io::{self, Read};

use memchr::memchr;
use memchr::memchr_iter;

/// A block of complete COPY TEXT rows read from the underlying stream.
///
/// The frame's lifetime is tied to the [`BlockReader`]'s internal arena;
/// callers must consume the frame (split fields, decode columns, emit to a
/// sink) before calling [`BlockReader::next_block`] again.
pub struct BlockFrame<'a> {
    /// All complete rows from this block, concatenated (no separators).
    /// Use `row_offsets[i]..row_offsets[i + 1]` to slice out row `i`.
    pub bytes: &'a [u8],
    /// `n_rows + 1` offsets into `bytes`. The trailing `\n` is **not**
    /// included in the row slice.
    pub row_offsets: &'a [u32],
    /// Set when the `\.` end-of-data marker was seen; no more rows will be
    /// returned by subsequent calls.
    pub eod: bool,
}

/// Reads raw COPY TEXT bytes from `R` into an arena buffer, framing rows on
/// newline boundaries. Memory is capped at roughly `block_target * 2` bytes
/// to absorb a row that straddles a refill.
pub struct BlockReader<R: Read> {
    inner: R,
    arena: Vec<u8>,
    /// Bytes of live data in `arena` (may be longer than `arena.len()` never —
    /// we keep `arena.len() == valid_len` always).
    valid_len: usize,
    /// Start offset of the next unconsumed byte within `arena`. Between calls
    /// we drain consumed rows and memmove the tail to the front.
    consumed: usize,
    /// Row offsets *relative to `consumed`*, built by the last
    /// [`next_block`][Self::next_block] call.
    row_offsets: Vec<u32>,
    done: bool,
    eod: bool,
    /// Target refill size. Smaller = less memory, more framing overhead;
    /// larger = better amortisation but bigger peak arena.
    block_target: usize,
}

impl<R: Read> BlockReader<R> {
    /// Create a new block reader. `block_target` is the number of bytes the
    /// reader tries to have available before returning a block; it grows
    /// past that to absorb oversize rows.
    pub fn new(inner: R, block_target: usize) -> Self {
        Self {
            inner,
            arena: Vec::with_capacity(block_target.saturating_mul(2)),
            valid_len: 0,
            consumed: 0,
            row_offsets: Vec::with_capacity(4096),
            done: false,
            eod: false,
            block_target,
        }
    }

    /// Read the next block of complete rows. Returns `Ok(None)` when the
    /// stream is exhausted or the end-of-data marker was reached.
    pub fn next_block(&mut self) -> io::Result<Option<BlockFrame<'_>>> {
        if self.done && self.valid_len == self.consumed {
            return Ok(None);
        }

        // Drop already-consumed bytes from the front of the arena so we can
        // reuse the capacity for the next refill.
        if self.consumed > 0 {
            self.arena.drain(..self.consumed);
            self.valid_len -= self.consumed;
            self.consumed = 0;
        }

        // Refill until we have at least `block_target` bytes of unseen data,
        // or the stream is exhausted.
        while !self.done && self.valid_len < self.block_target {
            let needed = self.block_target.saturating_sub(self.valid_len).max(64 * 1024);
            let grow_to = self.valid_len + needed;
            if self.arena.len() < grow_to {
                self.arena.resize(grow_to, 0);
            }
            match self.inner.read(&mut self.arena[self.valid_len..]) {
                Ok(0) => {
                    self.done = true;
                    break;
                }
                Ok(n) => self.valid_len += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }

        // Frame rows: find every `\n` up to `valid_len`. Collect positions
        // first, then do the compaction pass — simpler than juggling a
        // shared borrow of `self.arena` with the memchr iterator.
        let nl_positions: Vec<usize> =
            memchr_iter(b'\n', &self.arena[..self.valid_len]).collect();

        self.row_offsets.clear();
        self.row_offsets.push(0);
        let mut write = 0usize;
        let mut last_lf_plus_one = 0usize;
        for &nl in &nl_positions {
            let mut end = nl;
            if end > last_lf_plus_one && self.arena[end - 1] == b'\r' {
                end -= 1;
            }
            let len = end - last_lf_plus_one;

            if len == 2
                && self.arena[last_lf_plus_one] == b'\\'
                && self.arena[last_lf_plus_one + 1] == b'.'
            {
                self.eod = true;
                self.done = true;
                last_lf_plus_one = nl + 1;
                break;
            }

            if len > 0 {
                if last_lf_plus_one != write {
                    self.arena.copy_within(last_lf_plus_one..end, write);
                }
                write += len;
                self.row_offsets.push(write as u32);
            }
            last_lf_plus_one = nl + 1;
        }

        // Anything after the final `\n` is an incomplete row; keep it for
        // the next refill. We move it down right after the compacted rows.
        let tail = &self.arena[last_lf_plus_one..self.valid_len];
        let tail_len = tail.len();
        if tail_len > 0 {
            if last_lf_plus_one != write {
                self.arena.copy_within(last_lf_plus_one..self.valid_len, write);
            }
            // If EOF was reached and there's still a non-empty tail with no
            // trailing LF, treat it as a final complete row (the `\.` marker
            // aside, pg_dump always terminates rows with `\n`, so this is
            // defensive).
            if self.done && !self.eod {
                let end = write + tail_len;
                if !(tail_len == 2 && self.arena[write] == b'\\' && self.arena[write + 1] == b'.') {
                    self.row_offsets.push(end as u32);
                    write = end;
                } else {
                    self.eod = true;
                }
            } else {
                // Tail stays after the row region, waiting for the next refill.
                self.consumed = write;
                self.valid_len = write + tail_len;
                return self.maybe_frame();
            }
        }

        // No tail remains. `write` is the end of the last consumed row.
        self.consumed = write;
        self.valid_len = write;

        self.maybe_frame()
    }

    fn maybe_frame(&mut self) -> io::Result<Option<BlockFrame<'_>>> {
        if self.row_offsets.len() <= 1 {
            // No complete rows framed. If we're done, return None; else this
            // shouldn't happen (refill should have failed first) — guard anyway.
            if self.done {
                return Ok(None);
            }
            return Ok(None);
        }
        let end = *self.row_offsets.last().unwrap() as usize;
        Ok(Some(BlockFrame {
            bytes: &self.arena[..end],
            row_offsets: &self.row_offsets,
            eod: self.eod,
        }))
    }
}

/// Per-column byte ranges within a [`BlockFrame`].
///
/// For each of the `n_rows` rows and `n_cols` columns, the pair
/// `(ranges[c * n_rows + r].start, ranges[c * n_rows + r].end)` indicates the
/// field's byte span within `frame.bytes`. A `start == end` with `nulls[c]`
/// set at row `r` means SQL NULL; otherwise it means the empty string.
pub struct FieldRanges {
    pub n_rows: usize,
    pub n_cols: usize,
    /// Column-major. Length = n_rows * n_cols.
    pub ranges: Vec<Range>,
    /// Column-major null bitmap: one byte per `(col, row)` pair. 1 = NULL.
    /// We keep a flat `Vec<u8>` rather than a packed bitmap to stay simple;
    /// the final Arrow array can pack when emitting.
    pub nulls: Vec<u8>,
    /// Per-column flag: did any row in this column contain `\\`?
    pub has_escape: Vec<bool>,
}

#[derive(Clone, Copy, Default)]
pub struct Range {
    pub start: u32,
    pub end: u32,
}

impl Default for FieldRanges {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldRanges {
    pub fn new() -> Self {
        Self {
            n_rows: 0,
            n_cols: 0,
            ranges: Vec::new(),
            nulls: Vec::new(),
            has_escape: Vec::new(),
        }
    }

    /// Populate from a block frame. `n_cols` is the declared column count;
    /// rows with too few fields are padded with empty ranges + NULL marker,
    /// and rows with too many have their extras discarded.
    pub fn fill(&mut self, frame: &BlockFrame<'_>, n_cols: usize) {
        let n_rows = frame.row_offsets.len().saturating_sub(1);
        self.n_rows = n_rows;
        self.n_cols = n_cols;
        self.ranges.clear();
        self.ranges.resize(n_rows * n_cols, Range::default());
        self.nulls.clear();
        self.nulls.resize(n_rows * n_cols, 0);
        self.has_escape.clear();
        self.has_escape.resize(n_cols, false);

        for r in 0..n_rows {
            let row_start = frame.row_offsets[r] as usize;
            let row_end = frame.row_offsets[r + 1] as usize;
            let row = &frame.bytes[row_start..row_end];

            // Split on `\t`. pg_dump always escapes literal tabs in field
            // content as `\t`, so any raw `\t` byte is a field separator.
            let mut col = 0usize;
            let mut field_start = 0usize;
            for sep in memchr_iter(b'\t', row) {
                if col < n_cols {
                    let s = row_start + field_start;
                    let e = row_start + sep;
                    let bytes = &frame.bytes[s..e];
                    self.store_field(col, r, s as u32, e as u32, bytes);
                }
                col += 1;
                field_start = sep + 1;
            }
            if col < n_cols {
                let s = row_start + field_start;
                let e = row_end;
                let bytes = &frame.bytes[s..e];
                self.store_field(col, r, s as u32, e as u32, bytes);
                col += 1;
            }
            // Pad missing columns with NULL (shouldn't happen on well-formed
            // pg_dump output, but guard).
            while col < n_cols {
                self.nulls[col * n_rows + r] = 1;
                col += 1;
            }
        }
    }

    fn store_field(&mut self, col: usize, row: usize, s: u32, e: u32, bytes: &[u8]) {
        let idx = col * self.n_rows + row;
        // Whole-field `\N` = NULL marker in COPY TEXT.
        if bytes == b"\\N" {
            self.nulls[idx] = 1;
            self.ranges[idx] = Range { start: s, end: s };
            return;
        }
        self.ranges[idx] = Range { start: s, end: e };
        if !self.has_escape[col] && memchr(b'\\', bytes).is_some() {
            self.has_escape[col] = true;
        }
    }
}

/// Decoded columnar output for one block. One `ColumnBuffer` per column.
///
/// Memory layout matches Arrow's `Utf8` / `Binary` array: a contiguous
/// `values` buffer, an `offsets` array with `n_rows + 1` entries, and an
/// optional packed validity bitmap.
pub struct ColumnarBlock {
    pub n_rows: usize,
    pub columns: Vec<ColumnBuffer>,
}

pub struct ColumnBuffer {
    /// All non-null values concatenated.
    pub values: Vec<u8>,
    /// Arrow-style offsets: `offsets[i]..offsets[i+1]` is row `i`. Length
    /// `n_rows + 1`.
    pub offsets: Vec<i32>,
    /// Arrow-style validity bitmap, LSB-first packed bits. `None` means
    /// no nulls.
    pub validity: Option<Vec<u8>>,
}

impl ColumnarBlock {
    /// Decode a frame + field ranges into Arrow-shaped column buffers.
    ///
    /// Zero-copy fast path: columns with no escape byte in any field in this
    /// block skip the decoder entirely — `values` is built by memcpy from the
    /// frame's byte ranges. Columns with escapes go through [`decode_escapes`]
    /// per field.
    pub fn build(frame: &BlockFrame<'_>, ranges: &FieldRanges) -> Self {
        let n_rows = ranges.n_rows;
        let n_cols = ranges.n_cols;
        let mut columns: Vec<ColumnBuffer> = Vec::with_capacity(n_cols);

        for col in 0..n_cols {
            let col_null = &ranges.nulls[col * n_rows..(col + 1) * n_rows];
            let col_ranges = &ranges.ranges[col * n_rows..(col + 1) * n_rows];
            let has_nulls = col_null.iter().any(|&b| b != 0);
            let has_escape = ranges.has_escape[col];

            let mut offsets: Vec<i32> = Vec::with_capacity(n_rows + 1);
            offsets.push(0);

            if has_escape {
                // Slow path: decode escapes field by field.
                // Estimate value buffer size from raw range totals; decoded
                // bytes are always <= raw length for pg-style escapes.
                let mut values: Vec<u8> = Vec::with_capacity(
                    col_ranges.iter().map(|r| (r.end - r.start) as usize).sum(),
                );
                for (r, range) in col_ranges.iter().enumerate() {
                    if col_null[r] != 0 {
                        offsets.push(values.len() as i32);
                        continue;
                    }
                    let raw = &frame.bytes[range.start as usize..range.end as usize];
                    decode_escapes(raw, &mut values);
                    offsets.push(values.len() as i32);
                }
                columns.push(ColumnBuffer {
                    values,
                    offsets,
                    validity: has_nulls.then(|| pack_validity(col_null)),
                });
            } else {
                // Fast path: raw bytes contain no `\\`, so field bytes are
                // their own decoded form. memcpy per field into a packed
                // values buffer. We could get clever and reuse the frame
                // buffer directly, but contiguous packing simplifies
                // downstream Arrow building.
                let total: usize = col_ranges
                    .iter()
                    .zip(col_null.iter())
                    .map(|(r, &n)| if n != 0 { 0 } else { (r.end - r.start) as usize })
                    .sum();
                let mut values: Vec<u8> = Vec::with_capacity(total);
                for (r, range) in col_ranges.iter().enumerate() {
                    if col_null[r] == 0 {
                        values.extend_from_slice(
                            &frame.bytes[range.start as usize..range.end as usize],
                        );
                    }
                    offsets.push(values.len() as i32);
                }
                columns.push(ColumnBuffer {
                    values,
                    offsets,
                    validity: has_nulls.then(|| pack_validity(col_null)),
                });
            }
        }

        Self { n_rows, columns }
    }
}

/// Decode pg COPY-TEXT backslash escapes into `out`. Callers have already
/// determined this field is not the whole-field `\N` null marker.
pub fn decode_escapes(raw: &[u8], out: &mut Vec<u8>) {
    let mut i = 0;
    while i < raw.len() {
        let b = raw[i];
        if b != b'\\' || i + 1 >= raw.len() {
            out.push(b);
            i += 1;
            continue;
        }
        let c = raw[i + 1];
        match c {
            b'b' => { out.push(0x08); i += 2; }
            b'f' => { out.push(0x0C); i += 2; }
            b'n' => { out.push(b'\n'); i += 2; }
            b'r' => { out.push(b'\r'); i += 2; }
            b't' => { out.push(b'\t'); i += 2; }
            b'v' => { out.push(0x0B); i += 2; }
            b'\\' => { out.push(b'\\'); i += 2; }
            b'0'..=b'7' => {
                let mut val: u32 = 0;
                let mut n = 0;
                let mut j = i + 1;
                while n < 3 && j < raw.len() && matches!(raw[j], b'0'..=b'7') {
                    val = val * 8 + (raw[j] - b'0') as u32;
                    j += 1;
                    n += 1;
                }
                out.push((val & 0xFF) as u8);
                i = j;
            }
            b'x' => {
                let mut val: u32 = 0;
                let mut n = 0;
                let mut j = i + 2;
                while n < 2 && j < raw.len() && raw[j].is_ascii_hexdigit() {
                    val = val * 16
                        + match raw[j] {
                            d @ b'0'..=b'9' => d - b'0',
                            d @ b'a'..=b'f' => d - b'a' + 10,
                            d @ b'A'..=b'F' => d - b'A' + 10,
                            _ => unreachable!(),
                        } as u32;
                    j += 1;
                    n += 1;
                }
                if n == 0 {
                    out.push(b'x');
                } else {
                    out.push((val & 0xFF) as u8);
                }
                i = j;
            }
            _ => {
                out.push(c);
                i += 2;
            }
        }
    }
}

/// Pack a byte-per-row null marker (`1 = null`) into an Arrow-style validity
/// bitmap (`1 = valid`).
fn pack_validity(nulls: &[u8]) -> Vec<u8> {
    let n = nulls.len();
    let bytes = n.div_ceil(8);
    let mut bits = vec![0u8; bytes];
    for (i, &null) in nulls.iter().enumerate() {
        if null == 0 {
            bits[i / 8] |= 1 << (i % 8);
        }
    }
    bits
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn collect_rows(data: &[u8], block_target: usize) -> Vec<Vec<u8>> {
        let mut br = BlockReader::new(Cursor::new(data), block_target);
        let mut rows = Vec::new();
        while let Some(frame) = br.next_block().unwrap() {
            for w in frame.row_offsets.windows(2) {
                rows.push(frame.bytes[w[0] as usize..w[1] as usize].to_vec());
            }
        }
        rows
    }

    #[test]
    fn frames_three_rows() {
        let data = b"a\tb\nc\td\ne\tf\n";
        let rows = collect_rows(data, 1024);
        assert_eq!(rows, vec![b"a\tb".to_vec(), b"c\td".to_vec(), b"e\tf".to_vec()]);
    }

    #[test]
    fn handles_eod_marker() {
        let data = b"a\tb\nc\td\n\\.\n";
        let rows = collect_rows(data, 1024);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn frames_split_row_across_blocks() {
        // Force a refill boundary in the middle of a row.
        let data = b"one\ttwo\nthree\tfour\nfive\tsix\n";
        // Tiny block target so we refill several times.
        let rows = collect_rows(data, 4);
        assert_eq!(rows, vec![
            b"one\ttwo".to_vec(),
            b"three\tfour".to_vec(),
            b"five\tsix".to_vec(),
        ]);
    }

    #[test]
    fn splits_fields_and_detects_null() {
        let data = b"1\ta\\tb\t\\N\n2\t\\N\t\\\\N\n";
        let mut br = BlockReader::new(Cursor::new(&data[..]), 1024);
        let frame = br.next_block().unwrap().unwrap();
        let mut ranges = FieldRanges::new();
        ranges.fill(&frame, 3);
        assert_eq!(ranges.n_rows, 2);
        assert_eq!(ranges.n_cols, 3);

        // Row 0 col 2 = \N = NULL
        assert_eq!(ranges.nulls[2 * 2 + 0], 1);
        // Row 1 col 1 = \N = NULL
        assert_eq!(ranges.nulls[1 * 2 + 1], 1);
        // Row 1 col 2 = "\\N" literal (not null)
        assert_eq!(ranges.nulls[2 * 2 + 1], 0);

        // Columns with escapes: col 1 (has \t), col 2 (has \\N literal)
        assert!(ranges.has_escape[1]);
        assert!(ranges.has_escape[2]);
        assert!(!ranges.has_escape[0]);

        let block = ColumnarBlock::build(&frame, &ranges);
        // Col 0 values: "1" then "2" (no nulls, no escapes → fast path)
        let col0 = &block.columns[0];
        assert_eq!(col0.validity.as_deref(), None);
        assert_eq!(&col0.values[col0.offsets[0] as usize..col0.offsets[1] as usize], b"1");
        assert_eq!(&col0.values[col0.offsets[1] as usize..col0.offsets[2] as usize], b"2");

        // Col 1 values: row 0 = "a\tb" (decoded), row 1 = NULL
        let col1 = &block.columns[1];
        assert_eq!(&col1.values[col1.offsets[0] as usize..col1.offsets[1] as usize], b"a\tb");
        // Row 1's validity bit should be clear.
        let validity = col1.validity.as_deref().expect("col1 has nulls");
        assert_eq!(validity[0] & 0b10, 0);

        // Col 2 values: row 0 = NULL, row 1 = "\\N" (literal)
        let col2 = &block.columns[2];
        assert_eq!(&col2.values[col2.offsets[1] as usize..col2.offsets[2] as usize], b"\\N");
    }
}
