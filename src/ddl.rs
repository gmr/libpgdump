//! Minimal parser for the `CREATE TABLE` DDL statements pg_dump emits.
//!
//! pg_dump writes very regular DDL for tables — one column per line,
//! identifiers always double-quoted when they need to be, no comments inside
//! the column list. This parser exploits that regularity; it is not a general
//! SQL parser and will not handle arbitrary user-written DDL.

/// A parsed column definition — just enough to name and (later) type it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// The column name, with any wrapping double quotes stripped.
    pub name: String,
    /// The column type as it appears in the DDL (e.g. `"integer"`,
    /// `"character varying(255)"`, `"numeric(10,2)"`, `"text[]"`). Left as the
    /// original string so callers can map it to their own target type system.
    pub pg_type: String,
}

/// Extract the column list from a pg_dump `CREATE TABLE` statement.
///
/// Returns `None` if the statement is not a recognisable `CREATE TABLE` (e.g.
/// partitioned-only table declarations with `PARTITION OF` and no column
/// list, or non-table DDL accidentally routed here).
pub fn parse_create_table(defn: &str) -> Option<Vec<ColumnDef>> {
    // Find the opening paren of the column list. pg_dump writes
    //     CREATE TABLE [IF NOT EXISTS] [ONLY] schema.name (
    //         col1 type,
    //         col2 type,
    //         ...
    //     );
    // possibly followed by PARTITION BY / INHERITS / WITH etc. clauses.
    let open = find_top_level_open_paren(defn)?;
    let close = find_matching_close(&defn[open..])? + open;
    let inner = &defn[open + 1..close];

    let mut cols = Vec::new();
    for raw in split_top_level_commas(inner) {
        let item = raw.trim();
        if item.is_empty() {
            continue;
        }
        // Skip table-level constraints: CONSTRAINT ..., PRIMARY KEY (...),
        // FOREIGN KEY (...), UNIQUE (...), CHECK (...), EXCLUDE ..., LIKE ...
        let upper = item.to_ascii_uppercase();
        if upper.starts_with("CONSTRAINT ")
            || upper.starts_with("PRIMARY KEY")
            || upper.starts_with("FOREIGN KEY")
            || upper.starts_with("UNIQUE ")
            || upper.starts_with("UNIQUE(")
            || upper.starts_with("CHECK ")
            || upper.starts_with("CHECK(")
            || upper.starts_with("EXCLUDE ")
            || upper.starts_with("LIKE ")
        {
            continue;
        }
        if let Some(col) = parse_column(item) {
            cols.push(col);
        }
    }
    if cols.is_empty() { None } else { Some(cols) }
}

/// Parse a single column definition line: `name type [modifiers...]`.
fn parse_column(s: &str) -> Option<ColumnDef> {
    let s = s.trim();
    let (name, rest) = take_identifier(s)?;

    // Strip trailing modifiers that aren't part of the type. We want the
    // raw type as emitted by pg_dump, so we keep everything up to the first
    // top-level keyword that ends the type expression.
    let type_str = trim_trailing_modifiers(rest.trim());
    if type_str.is_empty() {
        return None;
    }
    Some(ColumnDef {
        name,
        pg_type: type_str.to_string(),
    })
}

/// Consume one identifier (quoted or unquoted) from the start of `s`,
/// returning `(identifier, rest)`.
fn take_identifier(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    if s.starts_with('"') {
        // Quoted identifier: "..." with "" for an embedded quote.
        let bytes = s.as_bytes();
        let mut i = 1;
        let mut out = String::new();
        while i < bytes.len() {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    out.push('"');
                    i += 2;
                    continue;
                }
                return Some((out, &s[i + 1..]));
            }
            out.push(bytes[i] as char);
            i += 1;
        }
        None
    } else {
        let end = s
            .find(|c: char| c.is_whitespace() || c == '(' || c == ',')
            .unwrap_or(s.len());
        if end == 0 {
            return None;
        }
        Some((s[..end].to_string(), &s[end..]))
    }
}

/// Strip per-column modifiers we don't want in the type string (DEFAULT,
/// NOT NULL, CONSTRAINT, COLLATE, GENERATED, REFERENCES, PRIMARY KEY, UNIQUE,
/// CHECK). Everything up to the first such keyword (matched at word
/// boundaries outside parens) is the type.
fn trim_trailing_modifiers(s: &str) -> &str {
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if b == b')' {
            depth = depth.saturating_sub(1);
            i += 1;
            continue;
        }
        if depth == 0 && (b == b' ' || b == b'\t') {
            // Look for a modifier keyword starting at i+1.
            let rest = &s[i..].trim_start();
            for kw in &[
                "DEFAULT ",
                "NOT NULL",
                "NULL ",
                "NULL,",
                "CONSTRAINT ",
                "COLLATE ",
                "GENERATED ",
                "REFERENCES ",
                "PRIMARY ",
                "UNIQUE ",
                "CHECK ",
                "CHECK(",
            ] {
                if rest.len() >= kw.len()
                    && rest.as_bytes()[..kw.len()].eq_ignore_ascii_case(kw.as_bytes())
                {
                    return s[..i].trim_end();
                }
            }
            // Handle trailing bare NULL / NOT NULL at end of line.
            let up = rest.to_ascii_uppercase();
            if up == "NULL" || up == "NOT NULL" {
                return s[..i].trim_end();
            }
        }
        i += 1;
    }
    s.trim_end()
}

fn find_top_level_open_paren(s: &str) -> Option<usize> {
    // Skip ahead to find the first `(` that's not inside an identifier or
    // string literal. pg_dump output is clean enough that the first '(' is
    // the column list opener for CREATE TABLE.
    let bytes = s.as_bytes();
    let mut in_str = false;
    let mut in_ident = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_ident => in_str = !in_str,
            b'"' if !in_str => in_ident = !in_ident,
            b'(' if !in_str && !in_ident => return Some(i),
            _ => {}
        }
        i += 1;
    }
    None
}

/// Given `s` starting with `(`, return the index (relative to `s`) of the
/// matching `)`.
fn find_matching_close(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    debug_assert_eq!(bytes[0], b'(');
    let mut depth: usize = 0;
    let mut in_str = false;
    let mut in_ident = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_ident => in_str = !in_str,
            b'"' if !in_str => in_ident = !in_ident,
            b'(' if !in_str && !in_ident => depth += 1,
            b')' if !in_str && !in_ident => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split `s` on top-level commas (ignoring commas inside parens / quotes).
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut depth: usize = 0;
    let mut in_str = false;
    let mut in_ident = false;
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_ident => in_str = !in_str,
            b'"' if !in_str => in_ident = !in_ident,
            b'(' if !in_str && !in_ident => depth += 1,
            b')' if !in_str && !in_ident => depth = depth.saturating_sub(1),
            b',' if depth == 0 && !in_str && !in_ident => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(&s[start..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_table() {
        let sql = "CREATE TABLE public.users (\n    id integer NOT NULL,\n    email text,\n    created_at timestamp with time zone\n);\n";
        let cols = parse_create_table(sql).unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].pg_type, "integer");
        assert_eq!(cols[1].name, "email");
        assert_eq!(cols[1].pg_type, "text");
        assert_eq!(cols[2].name, "created_at");
        assert_eq!(cols[2].pg_type, "timestamp with time zone");
    }

    #[test]
    fn quoted_identifier_and_parameterised_type() {
        let sql = r#"CREATE TABLE s.t (
    "weird Name" character varying(64),
    price numeric(10,2) DEFAULT 0 NOT NULL
);"#;
        let cols = parse_create_table(sql).unwrap();
        assert_eq!(cols[0].name, "weird Name");
        assert_eq!(cols[0].pg_type, "character varying(64)");
        assert_eq!(cols[1].name, "price");
        assert_eq!(cols[1].pg_type, "numeric(10,2)");
    }

    #[test]
    fn constraints_are_skipped() {
        let sql = "CREATE TABLE s.t (\n  id integer NOT NULL,\n  other text,\n  CONSTRAINT pk PRIMARY KEY (id),\n  PRIMARY KEY (id, other)\n);";
        let cols = parse_create_table(sql).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[1].name, "other");
    }

    #[test]
    fn array_type() {
        let sql = "CREATE TABLE s.t (tags text[]);";
        let cols = parse_create_table(sql).unwrap();
        assert_eq!(cols[0].pg_type, "text[]");
    }
}
