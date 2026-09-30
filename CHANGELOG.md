# dbt-gizmosql changelog

## v1.12.3 (2026-09-30)

### Features
- Python models can now return Arrow record batches in addition to full
  `pyarrow.Table`s: a single `pyarrow.RecordBatch`, a
  `pyarrow.RecordBatchReader`, a list or generator of `RecordBatch`es, or
  any object implementing the Arrow PyCapsule stream protocol
  (`__arrow_c_stream__`, e.g. a polars DataFrame). Readers, generators and
  stream objects are streamed to GizmoSQL via ADBC bulk ingest batch by
  batch instead of being collected into one table in client memory. Empty
  streams still create an (empty) table from the stream's schema.

### Dependency updates
- Bumped runtime dependency floors: `dbt-core` to `~=1.12.5`, `duckdb` to
  `>=1.5.6`, `pandas` to `>=3.0.6` (`dbt-common`, `dbt-adapters` and
  `adbc-driver-gizmosql` are already at their latest releases).
- Bumped dev-extra pin: `tox` to `>=4.64`.

### Test suite
- Added functional tests for each record batch return type (single batch,
  multi-batch reader, generator, list, `__arrow_c_stream__` object, empty
  and zero-row-batch streams) and for an incremental Python model that
  returns a generator of batches.
- Added the first unit tests (`tests/unit/`) covering how model results are
  normalized for ingest: which results are streamed vs. materialized, that
  generators are consumed lazily, empty-stream handling, and error messages
  for iterables that aren't record batches.
- The S3 `external` materialization test now runs its S3-compatible sidecar
  on the [Versity S3 Gateway](https://github.com/versity/versitygw)
  (`versity/versitygw:v1.8.0`, Apache-2.0) instead of MinIO, whose images are
  no longer pullable (Docker Hub denies `minio/minio`; Quay answers 401) —
  matching the gizmosql repo's CI. The test's S3 secret now sets `REGION`,
  which the gateway (like real S3) requires.

## v1.12.2 (2026-09-11)

### Bug fixes
- The package `__version__` (`dbt/adapters/gizmosql/__init__.py`,
  `__version__.py`) and the bundled `dbt_project.yml` version were left at
  `1.12.0` in the v1.12.1 release; all four version files now agree with
  `pyproject.toml`.

### Dependency updates
- Require `adbc-driver-gizmosql` >= 2.0.13. Picks up server-side query
  cancellation on Ctrl+C / statement close (2.0.9, 2.0.12), auto-prepare
  for bound parameters (2.0.10), and the 2.0.13 fix for parameterized
  DDL/DML issued via `cursor.execute(sql, params)` being silently lost.
- Bumped `dbt-common` to `~=1.39.0` (`dbt-core ~=1.12.0` now resolves to
  1.12.4, which requires `dbt-common >= 1.37.5`).
- Bumped dev-extra pins: `mypy` to `==2.3.1`, `tox` to `>=4.61`.

## v1.12.1 (2026-08-24)

### Dependency updates
- Require `adbc-driver-gizmosql` >= 2.0.8. v2.0.8 fixes geometry-aware bulk ingest against GizmoSQL >= 1.37.0 (which now creates `GEOMETRY` columns server-side); earlier driver builds fail there with `No function matches 'st_geomfromwkb(GEOMETRY)'`.

## v1.12.0 (2026-07-29)

### Dependency updates
- Bumped the `adbc-driver-gizmosql` minimum to `>=2.0.0`, powered by the
  new [native Go GizmoSQL ADBC driver](https://github.com/gizmodata/gizmosql-adbc).
  Same API as 1.x — GizmoSQL's DDL/DML immediate-execution (statement
  routing) handling, `RETURNING` support, `gizmosql://` URIs, and the
  OAuth/SSO flow now live in the shared Go driver library used across
  all languages; adapter behavior is unchanged.
- Bumped runtime dependency floors to current stable releases:
  `dbt-core` to `~=1.12.0`, `dbt-adapters` to `~=1.24.5`, `duckdb` to
  `>=1.5.5`, `pandas` to `>=3.0.0` (`dbt-common` stays at `~=1.38.0`,
  already latest).
- Bumped dev-extra pins: `dbt-tests-adapter` to `==1.20.*`, `black` to
  `==26.5.1`, `mypy` to `==2.3.0`, `tox` to `>=4.58`.

### CI
- Bumped GitHub Actions to current majors: `actions/checkout@v7`,
  `actions/setup-python@v7`, `actions/upload-artifact@v7`, and
  `softprops/action-gh-release@v3` (`pypa/gh-action-pypi-publish` stays
  on the floating `release/v1` branch).

## v1.11.16 (2026-05-10)

### Test suite
- Replaced the Docker-managed GizmoSQL test server with the new
  [`gizmosql`](https://pypi.org/project/gizmosql/) PyPI package. The test
  fixture now starts the server as a subprocess via
  `gizmosql.Server(...)`, which auto-picks a free port (enabling parallel
  pytest workers), guarantees client/server release parity, and drops the
  custom Docker log-poller. Local development no longer requires Docker
  Desktop running for the GizmoSQL server itself.
- Refactored `test_external.py`'s MinIO sidecar to bind to
  `localhost:9000` instead of routing through a user-defined Docker
  bridge network with container aliases — the GizmoSQL subprocess is now
  on the host, so the network gymnastics are no longer needed.
- Removed `test.env` and the `pytest-dotenv` dependency; test fixtures
  construct connection details directly from the `Server` object.

### Dependency updates
- Bumped `dbt-core` to `~=1.11.9`, `dbt-common` to `~=1.38.0`, and
  `dbt-adapters` to `~=1.23.0`.
- Bumped `adbc-driver-gizmosql` minimum to `>=1.1.6`.
- Added `gizmosql` as a dev dependency (the new test-fixture driver);
  `docker` is retained for the MinIO sidecar still used by
  `test_external.py`.

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

🙏 Thanks to: @maartenbosteels for the feature idea!

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
