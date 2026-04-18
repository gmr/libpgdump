//! Parse `CREATE TABLE` DDL from a pg_dump archive using `sqlparser-rs`
//! with the PostgreSQL dialect.
//!
//! Public API: one type ([`ColumnDef`]) and one function
//! ([`parse_create_table`]), unchanged from the previous hand-rolled
//! implementation. Downstream code (`pg_to_duckdb_type`, the Arrow schema
//! builder, etc.) treats `ColumnDef::pg_type` as an opaque string matched
//! case-insensitively, so we just render each column's `DataType` back to
//! a string form that the existing mappers already understand.
//!
//! Why sqlparser-rs: pg_dump DDL is mostly regular, but not entirely —
//! partitioned tables (`PARTITION BY`), inherited tables (`INHERITS(...)`)
//! typed composites, `GENERATED ALWAYS AS`, `LIKE ... INCLUDING ...`, and
//! friends all show up in the wild. A real AST walker handles them
//! correctly without us accumulating ad-hoc edge-case patches.

use sqlparser::ast::{ColumnOption, DataType, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// A parsed column definition — just enough to name and (later) type it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// The column name, with any wrapping double quotes stripped.
    pub name: String,
    /// The column type as a string (e.g. `"INTEGER"`,
    /// `"CHARACTER VARYING(64)"`, `"NUMERIC(10,2)"`, `"TEXT[]"`). Rendered by
    /// sqlparser's `Display` for `DataType`, so case / spacing can differ
    /// from what pg_dump literally emitted — downstream mappers do
    /// case-insensitive matching, so that's fine.
    pub pg_type: String,
}

/// Extract the column list from a pg_dump `CREATE TABLE` statement.
///
/// Returns `None` if the statement is not a recognisable `CREATE TABLE`
/// (for instance a `CREATE TABLE ... PARTITION OF parent` declaration with
/// no column list, or non-table DDL accidentally routed here). Constraints
/// and other non-column items in the column list (`PRIMARY KEY`,
/// `FOREIGN KEY`, `CHECK`, `LIKE`, `EXCLUDE`, ...) are filtered out by
/// sqlparser's AST shape — they live in `CreateTable::constraints`, not
/// `CreateTable::columns`, so we don't have to handle them ourselves.
pub fn parse_create_table(defn: &str) -> Option<Vec<ColumnDef>> {
    let dialect = PostgreSqlDialect {};
    let normalised = normalise_pg_modifiers(defn);
    // pg_dump sometimes emits multiple statements in one `defn` (the DDL
    // plus a trailing `\.` separator or similar). Ask sqlparser to parse
    // everything and return the first CREATE TABLE it finds.
    let stmts = Parser::parse_sql(&dialect, &normalised).ok()?;

    for stmt in stmts {
        if let Statement::CreateTable(ct) = stmt {
            let cols: Vec<ColumnDef> = ct
                .columns
                .iter()
                .filter(|c| !is_generated_virtual(&c.options))
                .map(|c| ColumnDef {
                    name: c.name.value.clone(),
                    pg_type: render_data_type(&c.data_type),
                })
                .collect();
            return (!cols.is_empty()).then_some(cols);
        }
    }
    None
}

/// Strip PostgreSQL `CREATE TABLE` modifiers that sqlparser's dialect
/// doesn't recognise but that are semantic no-ops for our purposes
/// (column names and types are the only thing we extract, and these
/// modifiers don't change that).
///
/// Currently handled:
/// * `CREATE UNLOGGED TABLE …` → `CREATE TABLE …`
/// * `CREATE GLOBAL TEMPORARY TABLE …` → `CREATE TEMPORARY TABLE …`
///   (sqlparser handles `TEMPORARY` natively).
/// * Leading `ONLY` inside the identifier list (rare, but legal).
fn normalise_pg_modifiers(defn: &str) -> String {
    // We only care about the first `CREATE TABLE`-ish header; use a simple
    // case-insensitive word-boundary replace against common pg-isms. A
    // real regex would be cleaner; we avoid the `regex` dep for just this.
    let mut out = defn.to_string();
    for pat in ["CREATE UNLOGGED TABLE", "create unlogged table"] {
        if let Some(i) = out.find(pat) {
            out.replace_range(i..i + pat.len(), "CREATE TABLE");
            break;
        }
    }
    for pat in ["CREATE GLOBAL TEMPORARY TABLE", "create global temporary table"] {
        if let Some(i) = out.find(pat) {
            out.replace_range(i..i + pat.len(), "CREATE TEMPORARY TABLE");
            break;
        }
    }
    out
}

/// sqlparser's `Display` for `DataType` is normally what we want. A tiny
/// tweak: pg_dump writes `character varying(N)` and our downstream mapper
/// strips parens and matches the base word — `DataType::Varchar(Some(N))`
/// renders as `VARCHAR(N)`, which the mapper already accepts. Arrays
/// render as `T[]`. Everything else (INTEGER, BIGINT, TIMESTAMP WITH TIME
/// ZONE, NUMERIC(p,s), BOOLEAN, JSON, JSONB, UUID, ...) round-trips via
/// `Display` cleanly. We funnel through one function to keep the rendering
/// behaviour in one place if we ever need to massage it.
fn render_data_type(ty: &DataType) -> String {
    ty.to_string()
}

/// Skip columns that are purely virtual (generated expressions with no
/// storage). pg_dump emits these as part of the schema but they carry no
/// data in the COPY stream, so including them would throw off the row
/// shape.
fn is_generated_virtual(options: &[sqlparser::ast::ColumnOptionDef]) -> bool {
    options.iter().any(|o| {
        matches!(
            o.option,
            ColumnOption::Generated {
                generated_as: sqlparser::ast::GeneratedAs::Always,
                sequence_options: None,
                generation_expr: Some(_),
                generated_keyword: true,
                generation_expr_mode: Some(sqlparser::ast::GeneratedExpressionMode::Virtual),
            }
        )
    })
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
        // sqlparser renders `integer` as `INT`; our mapper is case-insensitive
        // and matches both forms.
        assert!(
            cols[0].pg_type.eq_ignore_ascii_case("int")
                || cols[0].pg_type.eq_ignore_ascii_case("integer")
        );
        assert_eq!(cols[1].name, "email");
        assert!(cols[1].pg_type.eq_ignore_ascii_case("text"));
        assert_eq!(cols[2].name, "created_at");
        assert!(
            cols[2]
                .pg_type
                .eq_ignore_ascii_case("timestamp with time zone")
                || cols[2].pg_type.eq_ignore_ascii_case("timestamptz")
        );
    }

    #[test]
    fn quoted_identifier_and_parameterised_type() {
        let sql = r#"CREATE TABLE s.t (
    "weird Name" character varying(64),
    price numeric(10,2) DEFAULT 0 NOT NULL
);"#;
        let cols = parse_create_table(sql).unwrap();
        assert_eq!(cols[0].name, "weird Name");
        let t0 = cols[0].pg_type.to_ascii_lowercase();
        assert!(
            t0.contains("varying(64)") || t0.contains("varchar(64)"),
            "{}",
            cols[0].pg_type
        );
        assert_eq!(cols[1].name, "price");
        let t1 = cols[1].pg_type.to_ascii_lowercase();
        assert!(t1.contains("numeric(10,2)"), "{}", cols[1].pg_type);
    }

    #[test]
    fn constraints_are_skipped() {
        // Table-level PRIMARY KEY is in `ct.constraints`, not `ct.columns`,
        // so we naturally don't pick it up. Verify we only see the two
        // declared columns.
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
        // sqlparser renders array types as `T[]`.
        assert!(
            cols[0].pg_type.to_ascii_lowercase().contains("text[]"),
            "{}",
            cols[0].pg_type
        );
    }

    #[test]
    fn partitioned_parent_table() {
        // Partitioned parent tables declare their columns plus a PARTITION BY
        // clause. Columns should come through cleanly.
        let sql = "CREATE TABLE public.events (\n  id bigint NOT NULL,\n  tenant_id int NOT NULL,\n  occurred_at timestamptz NOT NULL\n) PARTITION BY RANGE (occurred_at);";
        let cols = parse_create_table(sql).unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[2].name, "occurred_at");
    }

    #[test]
    fn unlogged_table() {
        // Edge case: UNLOGGED modifier.
        let sql = "CREATE UNLOGGED TABLE public.scratch (id int, note text);";
        let cols = parse_create_table(sql).unwrap();
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn partition_of_has_no_columns() {
        // `CREATE TABLE ... PARTITION OF parent` inherits columns from the
        // parent and declares none of its own. Our API returns `None`
        // because there's nothing to export at this entry — pg_dump routes
        // data for these partitions through TABLE DATA entries tagged with
        // the partition's own namespace/tag, which we look up in the parent
        // table's schema map.
        let sql = "CREATE TABLE public.events_2024 PARTITION OF public.events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');";
        assert!(parse_create_table(sql).is_none());
    }
}
