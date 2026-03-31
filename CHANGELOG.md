# dbt-gizmosql changelog

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
