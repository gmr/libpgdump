# pgdump2parquet — planned follow-ups

A grab-bag of work that's been scoped and deferred. Each item has enough
shape here that a future contributor (or re-invoked LLM) can pick it up
without re-doing the design.

## PostGIS → GeoParquet

Convert a pg_dump of a PostGIS database directly to valid GeoParquet, entirely
in-process.

**Why it works**: pg_dump emits PostGIS `geometry` / `geography` columns as
**hex-encoded EWKB** in COPY TEXT (for example `0101000020E6100000...`). That
is exactly what DuckDB's spatial extension consumes via `ST_GeomFromHEXEWKB`.
With DuckDB's spatial extension loaded, a `GEOMETRY` column written to Parquet
gets the GeoParquet `geo` metadata automatically.

**Sketch of the pipeline for a PostGIS table**:

```
pg_dump COPY TEXT ── "0101000020E6100000..."  ──▶  DuckDB staging VARCHAR
                                                           │
                                LOAD spatial               ▼
                          COPY (SELECT ST_GeomFromHEXEWKB(wkb) AS geom,
                                       TRY_CAST(other AS target), ...
                                FROM _stage)
                          TO 'x.parquet' (FORMAT parquet);
                                                           │
                                                           ▼
                                GeoParquet: WKB geometry column + `geo` metadata
                                (CRS from the EWKB SRID prefix)
```

**Work required in `pgdump2parquet-duckdb`**:

1. Type detection: the DDL parser already captures `pg_type`. Add a predicate
   `is_geo_type(&str) -> bool` matching `^geometry(\(...\))?$` and
   `^geography(\(...\))?$` (case-insensitive).
2. Schema-level flag: if any column in the `TableSchema` is geo, flag the
   sink as "geo-enabled" at `open()` time.
3. Load the extension once per geo-enabled sink:
   ```sql
   INSTALL spatial;
   LOAD spatial;
   ```
   `duckdb-rs` has the spatial extension bundled in recent versions — verify
   with `SELECT * FROM duckdb_extensions() WHERE extension_name='spatial'`.
4. Cast dispatch: in the `COPY (SELECT ...)` builder, replace the usual
   `TRY_CAST(col AS GEOMETRY)` with `ST_GeomFromHEXEWKB(col) AS col` for
   geo columns. The rest of the CAST path is unchanged.
5. Optional: detect PostGIS presence on the *dump* side by scanning the TOC
   for an `ObjectType::Extension` entry with tag `postgis`, or a
   `public.spatial_ref_sys` `TABLE` entry. If present, enable geo handling
   automatically; otherwise leave it off. Users can force the behavior with
   a `--geo-columns schema.table.col` flag for edge cases.

**Work required for the Arrow backend**: leave geo as plain `VARCHAR`
(hex-EWKB). Users who want GeoParquet from the Arrow path can run a separate
DuckDB pass on the resulting Parquet. Writing GeoParquet directly from arrow-rs
requires hand-assembling the `geo` metadata JSON and a WKB `BinaryArray` — out
of scope for now.

**Testing**: a tiny PostGIS fixture (1 table, 10 rows with POINT/POLYGON in
4326) is enough to verify the full path. The `postgis` extension is available
in the Ubuntu `postgresql-postgis` package used in dev.

## Within-table parallelism for the 400GB table

Today a single table is processed by a single worker. For the monster use
case, decompress on one thread and parse+emit on another through a bounded
channel. Sketch:

* Thread A owns the `BlockReader` + decompressor.
* Thread B owns the sink.
* Bounded `crossbeam::channel::bounded(4)` between them carries *owned*
  `ColumnarBlock`s.
* At high zstd levels the decompressor is the bottleneck; at low levels
  the encoder is. Either way, overlapping them recovers ~1.3–1.8× on
  a single big table.

## Multi-part output per table

For tables that don't fit in memory at close time (DuckDB backend,
predominantly), add `--parts-rows R` so output becomes
`table.part000.parquet`, `table.part001.parquet`, ... Each part stages,
copies, and clears `_stage` so DuckDB's working set stays bounded. Readers
query `read_parquet('table.part*.parquet')`.

## Type inference from data when DDL is absent / lying

Today we trust the `CREATE TABLE` DDL. A future `--infer-types` mode could
sniff the first N rows per column and pick the narrowest compatible type —
useful for dumps where the DDL declares `text` but the column is really
integers.
