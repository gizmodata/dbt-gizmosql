# dbt-gizmosql changelog

## v1.11.15 (2026-04-11)

### Features
- **`session.remote_sql()` — server-side pushdown for Python models.**
  Python models run client-side, so `dbt.ref('big_table').filter(...)`
  streams the entire upstream table across the wire before filtering
  locally. The new `session.remote_sql(query)` escape hatch runs arbitrary
  SQL on the GizmoSQL server over the existing ADBC connection and
  returns only the result as a local DuckDB relation, so filters and
  aggregations execute server-side and only the matching rows cross the
  network:

  ```python
  def model(dbt, session):
      dbt.config(materialized="table")
      schema = dbt.this.schema
      return session.remote_sql(
          f"select * from {schema}.big_table where name = 'Joe'"
      )
  ```

  The `session` arg is now a `_GizmoSQLSession` proxy that delegates every
  attribute to the underlying local DuckDB connection (`session.sql()`,
  `session.register()`, etc. keep working unchanged) and adds
  `remote_sql()` on top — additive, not a replacement. `remote_sql()`
  returns a chainable relation you can combine with `.filter()`,
  `.project()`, `.df()`, pandas, etc., or return directly from the model.

### Test suite
- New `TestPythonRemoteSQLPushdown` functional test: materializes a
  multi-row upstream table and a Python model that uses
  `session.remote_sql()` with a `WHERE` filter, then asserts the output
  table contains exactly the server-filtered rows.

## v1.11.14 (2026-04-11)

### Bug fixes
- Fixed `dbt show -s <python_model>` crashing with
  `AttributeError: 'NoneType' object has no attribute 'print_table'` (#6).
  Root cause: dbt's default `get_limit_sql` wraps `compiled_code` in
  `LIMIT N` and executes it, but Python models have Python source as
  `compiled_code` so the wrapped string fails to parse as SQL
  (`Parser Error: syntax error at or near "def"`). The runner catches that
  error but still stores `agate_table=None`, and `task_end_messages`
  dereferences it without a status check — the NoneType traceback is just
  the tombstone on top of the real parse failure. Override
  `gizmosql__get_limit_sql` to detect Python models via `model.language`
  and `select * from {{ this }} limit N` from the already-materialized
  target relation instead.

### Test suite
- New `TestShowPythonModelGizmoSQL` regression test covering standalone
  Python model, Python model calling `dbt.ref()`, a plain SQL model (guard
  against breaking the default path), and Python model + `--limit`.

## v1.11.13 (2026-04-11)

### Features
- **External materialization** — write dbt models directly to Parquet,
  CSV, or JSON files via server-side `COPY` statements. Because GizmoSQL
  is remote DuckDB, the `COPY` runs on the GizmoSQL server (typically a
  cloud VM with more CPU, memory, disk throughput, NIC bandwidth, and
  cloud-IAM reach than the dbt client) rather than streaming result sets
  back to the client just to write them out. Supports:
  - Local filesystem paths and any URI the server's DuckDB backend can
    reach: `s3://`, `gs://`, `azure://`, MinIO and other S3-compatible
    stores, etc.
  - Format inference from file extension, explicit `format` config, custom
    `delimiter`, and arbitrary DuckDB `COPY` `options` (compression
    codecs, `partition_by`, `per_thread_output`, ...).
  - Default `{external_root}/{model_name}.{format}` locations via a new
    `external_root` profile setting (resolved on the server).
  - `ref()`-able — a view is created over the written file so downstream
    models can use it like any other relation.
- New adapter-level `@available` helpers: `external_root`,
  `external_write_options`, `external_read_location`, `location_exists`,
  `store_relation`, `warn_once`.
- New Jinja macros: `materializations/external.sql` and
  `utils/external_location.sql`.
- `plugin` / `glue_register` options (client-side features in dbt-duckdb)
  produce a clear compile-time error — they have no analogue in a
  server-side Flight SQL adapter.
- README: new "Writing to External Files (server-side)" section with the
  config table, profile example, partitioning example, and notes on the
  parent-directory requirement for local file writes.

### Test suite
- New `tests/functional/adapter/test_external.py` (7 test classes):
  default parquet, CSV, JSON, explicit `.parquet` location, explicit
  `.csv` location + custom delimiter, downstream `ref()`, empty-result
  handling, plugin/glue rejection, parquet compression codec, hive-
  partitioned writes, and an end-to-end S3 test using a MinIO sidecar on
  a user-defined docker bridge network.

## v1.11.11 (2026-03-31)

### Changes
- Removed adapter-side `adbc_get_info()` thread-safety workaround — now handled
  upstream in `adbc-driver-gizmosql` >= 1.1.5.

### Dependency updates
- `adbc-driver-gizmosql`: >=1.1.4 -> >=1.1.5

## v1.11.10 (2026-03-31)

### Bug fixes
- Fixed sporadic `"Catalog Error: Table ... does not exist"` failures on remote
  GizmoSQL instances. Root cause: dbt's query comment prefix (`/* ... */`)
  prevented the GizmoSQL ADBC driver from detecting DDL/DML statements, causing
  them to go through Flight SQL's `PREPARE` path instead of `execute_update()`.
  The fix is in `adbc-driver-gizmosql` >= 1.1.4. Adapter-side changes:
  - Disabled explicit transactions (`BEGIN`/`COMMIT` are now no-ops) to rely on
    autocommit, matching dbt-duckdb's approach.
  - Synced stale versions in `__version__.py` and `dbt_project.yml`.

### Test suite
- Added 68 tests from the dbt-core adapter test suite (78 total, up from 10):
  aliases, caching, concurrency, dbt_show, ephemeral, hooks, relations,
  simple_seed, simple_snapshot, store_test_failures, unit_testing, and
  utility macros/data types.

### CI
- Bumped `actions/checkout` to v4 and `actions/setup-python` to v5 (Node.js 24).

## v1.11.9 (2026-03-30)

### Bug fixes
- Fixed concurrency bug where table materializations would fail with
  `"Table with name <model>__dbt_tmp does not exist!"` during the rename step.
  Root cause: the `CHECKPOINT` command issued after every `COMMIT` acquired an
  exclusive lock that interfered with concurrent and subsequent transactions
  under DuckDB's snapshot isolation. Removed `CHECKPOINT` from `add_commit_query()`
  — committed data is immediately visible to other connections through DuckDB's
  WAL without it.
- Fixed test fixture to reuse an already-running GizmoSQL container when
  present, avoiding Docker port-conflict errors during local iterative testing.

### Dependency updates
- `dbt-core`: ~=1.11.6 -> ~=1.11.7
- `dbt-adapters`: ~=1.22.6 -> ~=1.22.9
- `freezegun` (dev): 1.4.0 -> 1.5.5

## v1.11.8 (2025-12-15)
- Add `auth_type` (OAuth/SSO) support via `"external"` authentication type.

## v1.11.7 (2025-11-20)
- Version bump. Added CLAUDE.md project guide.
