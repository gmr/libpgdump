use std::io::{Read, Write};

use crate::error::{Error, Result};
use crate::types::OffsetState;

/// Read a single byte.
pub fn read_byte<R: Read>(r: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

/// Write a single byte.
pub fn write_byte<W: Write>(w: &mut W, byte: u8) -> Result<()> {
    w.write_all(&[byte])?;
    Ok(())
}

/// Read a pg_dump integer.
///
/// Format: 1 sign byte (0=positive, 1=negative) + `int_size` magnitude bytes (LSB first).
pub fn read_int<R: Read>(r: &mut R, int_size: u8) -> Result<i32> {
    let sign = read_byte(r)?;
    let mut val: u32 = 0;
    for i in 0..int_size {
        let b = read_byte(r)? as u32;
        val |= b << (i * 8);
    }
    if sign != 0 {
        // Negate using wrapping arithmetic to handle i32::MIN correctly
        Ok((-(val as i64)) as i32)
    } else {
        Ok(val as i32)
    }
}

/// Write a pg_dump integer.
///
/// Format: 1 sign byte (0=positive, 1=negative) + `int_size` magnitude bytes (LSB first).
pub fn write_int<W: Write>(w: &mut W, value: i32, int_size: u8) -> Result<()> {
    let sign: u8 = if value < 0 { 1 } else { 0 };
    write_byte(w, sign)?;
    let magnitude = value.unsigned_abs();
    for i in 0..int_size {
        write_byte(w, ((magnitude >> (i * 8)) & 0xFF) as u8)?;
    }
    Ok(())
}

/// Read a pg_dump length-prefixed string.
///
/// Returns `None` for length <= 0 (NULL strings use -1, but we treat 0 the same).
pub fn read_string<R: Read>(r: &mut R, int_size: u8) -> Result<Option<String>> {
    let len = read_int(r, int_size)?;
    if len <= 0 {
        return Ok(None);
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    Ok(Some(String::from_utf8(buf)?))
}

/// Write a pg_dump length-prefixed string.
///
/// `None` is written as length -1. `Some("")` is written as length 0.
pub fn write_string<W: Write>(w: &mut W, value: Option<&str>, int_size: u8) -> Result<()> {
    match value {
        None => write_int(w, -1, int_size),
        Some("") => write_int(w, 0, int_size),
        Some(s) => {
            write_int(w, s.len() as i32, int_size)?;
            w.write_all(s.as_bytes())?;
            Ok(())
        }
    }
}

/// Read a pg_dump file offset.
///
/// Format: 1 state byte + `off_size` offset bytes (LSB first).
pub fn read_offset<R: Read>(r: &mut R, off_size: u8) -> Result<(OffsetState, u64)> {
    let state_byte = read_byte(r)?;
    let state = OffsetState::from_byte(state_byte)
        .ok_or_else(|| Error::DataIntegrity(format!("invalid offset state byte: {state_byte}")))?;
    let mut offset: u64 = 0;
    for i in 0..off_size {
        let b = read_byte(r)? as u64;
        offset |= b << (i * 8);
    }
    Ok((state, offset))
}

/// Write a pg_dump file offset.
///
/// Format: 1 state byte + `off_size` offset bytes (LSB first).
pub fn write_offset<W: Write>(
    w: &mut W,
    state: OffsetState,
    offset: u64,
    off_size: u8,
) -> Result<()> {
    write_byte(w, state as u8)?;
    for i in 0..off_size {
        write_byte(w, ((offset >> (i * 8)) & 0xFF) as u8)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read_write_byte() {
        let mut buf = Vec::new();
        write_byte(&mut buf, 0x42).unwrap();
        assert_eq!(read_byte(&mut Cursor::new(&buf)).unwrap(), 0x42);
    }

    #[test]
    fn test_read_write_int_positive() {
        let mut buf = Vec::new();
        write_int(&mut buf, 42, 4).unwrap();
        assert_eq!(buf.len(), 5); // 1 sign + 4 data
        assert_eq!(read_int(&mut Cursor::new(&buf), 4).unwrap(), 42);
    }

    #[test]
    fn test_read_write_int_negative() {
        let mut buf = Vec::new();
        write_int(&mut buf, -99, 4).unwrap();
        assert_eq!(read_int(&mut Cursor::new(&buf), 4).unwrap(), -99);
    }

    #[test]
    fn test_read_write_int_zero() {
        let mut buf = Vec::new();
        write_int(&mut buf, 0, 4).unwrap();
        assert_eq!(read_int(&mut Cursor::new(&buf), 4).unwrap(), 0);
    }

    #[test]
    fn test_read_write_int_max() {
        let mut buf = Vec::new();
        write_int(&mut buf, i32::MAX, 4).unwrap();
        assert_eq!(read_int(&mut Cursor::new(&buf), 4).unwrap(), i32::MAX);
    }

    #[test]
    fn test_read_write_int_min() {
        // i32::MIN is tricky because its absolute value overflows i32.
        // The format uses unsigned magnitude, so -2147483648 should round-trip.
        let mut buf = Vec::new();
        write_int(&mut buf, i32::MIN, 4).unwrap();
        assert_eq!(read_int(&mut Cursor::new(&buf), 4).unwrap(), i32::MIN);
    }

    #[test]
    fn test_read_write_string_some() {
        let mut buf = Vec::new();
        write_string(&mut buf, Some("hello"), 4).unwrap();
        assert_eq!(
            read_string(&mut Cursor::new(&buf), 4).unwrap(),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_read_write_string_none() {
        let mut buf = Vec::new();
        write_string(&mut buf, None, 4).unwrap();
        assert_eq!(read_string(&mut Cursor::new(&buf), 4).unwrap(), None);
    }

    #[test]
    fn test_read_write_string_empty() {
        let mut buf = Vec::new();
        write_string(&mut buf, Some(""), 4).unwrap();
        // Empty string writes length 0, which reads back as None
        assert_eq!(read_string(&mut Cursor::new(&buf), 4).unwrap(), None);
    }

    #[test]
    fn test_read_write_string_unicode() {
        let mut buf = Vec::new();
        write_string(&mut buf, Some("hello \u{1F600} world"), 4).unwrap();
        assert_eq!(
            read_string(&mut Cursor::new(&buf), 4).unwrap(),
            Some("hello \u{1F600} world".to_string())
        );
    }

    #[test]
    fn test_read_write_offset_set() {
        let mut buf = Vec::new();
        write_offset(&mut buf, OffsetState::Set, 12345, 8).unwrap();
        assert_eq!(buf.len(), 9); // 1 state + 8 offset
        let (state, offset) = read_offset(&mut Cursor::new(&buf), 8).unwrap();
        assert_eq!(state, OffsetState::Set);
        assert_eq!(offset, 12345);
    }

    #[test]
    fn test_read_write_offset_no_data() {
        let mut buf = Vec::new();
        write_offset(&mut buf, OffsetState::NoData, 0, 8).unwrap();
        let (state, offset) = read_offset(&mut Cursor::new(&buf), 8).unwrap();
        assert_eq!(state, OffsetState::NoData);
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_read_write_offset_large() {
        let mut buf = Vec::new();
        let big_offset = 0x00FF_FFFF_FFFF_FFFFu64;
        write_offset(&mut buf, OffsetState::Set, big_offset, 8).unwrap();
        let (state, offset) = read_offset(&mut Cursor::new(&buf), 8).unwrap();
        assert_eq!(state, OffsetState::Set);
        assert_eq!(offset, big_offset);
    }

    #[test]
    fn test_multiple_ints_sequential() {
        let mut buf = Vec::new();
        write_int(&mut buf, 1, 4).unwrap();
        write_int(&mut buf, 2, 4).unwrap();
        write_int(&mut buf, -3, 4).unwrap();

        let mut cursor = Cursor::new(&buf);
        assert_eq!(read_int(&mut cursor, 4).unwrap(), 1);
        assert_eq!(read_int(&mut cursor, 4).unwrap(), 2);
        assert_eq!(read_int(&mut cursor, 4).unwrap(), -3);
    }
}
