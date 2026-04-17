//! Parser for PostgreSQL `COPY ... TO ... TEXT` (a.k.a. pg_dump TEXT) row format.
//!
//! Spec (from the PostgreSQL docs):
//! * Rows are terminated by LF (`\n`).
//! * Fields are separated by TAB (`\t`).
//! * A field equal to the two bytes `\N` represents SQL NULL.
//! * Backslash escapes inside fields: `\b \f \n \r \t \v \\`, plus octal
//!   (`\OOO`, 1–3 octal digits) and hex (`\xHH`, 1–2 hex digits). Any other
//!   `\c` represents the character `c`.
//! * The literal terminator line `\.` marks end-of-data and is not a row.

/// Parse one COPY TEXT row into its field values.
///
/// `line` must not include the trailing LF. Returns one `Option<Vec<u8>>` per
/// field; `None` means SQL NULL. Field bytes are the decoded payload (not
/// guaranteed to be valid UTF-8 — pg_dump emits raw bytes for `bytea` etc.).
pub fn parse_line(line: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut fields: Vec<Option<Vec<u8>>> = Vec::new();
    // Find field boundaries (unescaped tabs) first, then decode each field.
    // Tabs only split when they're NOT preceded by an odd number of backslashes;
    // pg_dump COPY-TEXT does not emit raw tabs in field contents (they are
    // written as `\t`), so this is trivial: split on every literal tab byte.
    let mut field_start = 0;
    let mut i = 0;
    while i < line.len() {
        if line[i] == b'\t' {
            fields.push(decode_field(&line[field_start..i]));
            field_start = i + 1;
        }
        i += 1;
    }
    fields.push(decode_field(&line[field_start..]));
    fields
}

/// Decode one COPY TEXT field. Returns `None` if the raw bytes are exactly
/// `\N` (the NULL marker). Otherwise applies the backslash-escape decoding.
fn decode_field(raw: &[u8]) -> Option<Vec<u8>> {
    if raw == b"\\N" {
        return None;
    }

    let mut out = Vec::with_capacity(raw.len());
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
            b'b' => {
                out.push(0x08);
                i += 2;
            }
            b'f' => {
                out.push(0x0C);
                i += 2;
            }
            b'n' => {
                out.push(b'\n');
                i += 2;
            }
            b'r' => {
                out.push(b'\r');
                i += 2;
            }
            b't' => {
                out.push(b'\t');
                i += 2;
            }
            b'v' => {
                out.push(0x0B);
                i += 2;
            }
            b'\\' => {
                out.push(b'\\');
                i += 2;
            }
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
                // Any other `\c` represents the character c itself — including
                // `\N` *inside* a longer field (it only marks NULL as a whole
                // field, which is handled above).
                out.push(c);
                i += 2;
            }
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_fields() {
        let row = parse_line(b"1\thello\tworld");
        assert_eq!(row.len(), 3);
        assert_eq!(row[0], Some(b"1".to_vec()));
        assert_eq!(row[1], Some(b"hello".to_vec()));
        assert_eq!(row[2], Some(b"world".to_vec()));
    }

    #[test]
    fn null_field() {
        let row = parse_line(b"1\t\\N\tx");
        assert_eq!(row[1], None);
        assert_eq!(row[0], Some(b"1".to_vec()));
        assert_eq!(row[2], Some(b"x".to_vec()));
    }

    #[test]
    fn escapes() {
        let row = parse_line(b"a\\tb\ta\\nb\ta\\\\b");
        assert_eq!(row[0], Some(b"a\tb".to_vec()));
        assert_eq!(row[1], Some(b"a\nb".to_vec()));
        assert_eq!(row[2], Some(b"a\\b".to_vec()));
    }

    #[test]
    fn literal_backslash_n_in_field() {
        // "\\N" (four bytes) = literal "\N" text, NOT null.
        let row = parse_line(b"\\\\N");
        assert_eq!(row[0], Some(b"\\N".to_vec()));
    }

    #[test]
    fn empty_vs_null() {
        let row = parse_line(b"\t\\N\t");
        assert_eq!(row[0], Some(Vec::new()));
        assert_eq!(row[1], None);
        assert_eq!(row[2], Some(Vec::new()));
    }

    #[test]
    fn octal_escape() {
        let row = parse_line(b"\\101");
        assert_eq!(row[0], Some(b"A".to_vec()));
    }

    #[test]
    fn hex_escape() {
        let row = parse_line(b"\\x41");
        assert_eq!(row[0], Some(b"A".to_vec()));
    }
}
