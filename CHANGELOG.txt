# dbt-gizmosql changelog

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
