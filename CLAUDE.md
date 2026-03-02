# CLAUDE.md - Project Guide for Claude Code

## Project Overview
dbt-gizmosql is a [dbt](https://www.getdbt.com/) adapter for [GizmoSQL](https://gizmodata.com/gizmosql), an Apache Arrow Flight-based SQL engine with DuckDB and SQLite back-ends.

## Key Architecture
- **Driver**: Uses `adbc-driver-gizmosql` (not `adbc-driver-flightsql`) — the GizmoSQL driver wraps Flight SQL with a cleaner `connect()` API
- **Connection**: `dbt/adapters/gizmosql/connections.py` — `GizmoSQLCredentials` and `GizmoSQLConnectionManager`
- **Adapter**: `dbt/adapters/gizmosql/impl.py` — `GizmoSQLAdapter`
- **Plugin entry**: `dbt/adapters/gizmosql/__init__.py`

## Version Bumps
**IMPORTANT**: Version must be updated in TWO places:
1. `pyproject.toml` — `version = "x.y.z"`
2. `dbt/adapters/gizmosql/__init__.py` — `__version__ = "x.y.z"`

Version tracks dbt-core (e.g., dbt-core 1.11.x → dbt-gizmosql 1.11.x).

## CI/CD
- **Workflow**: `.github/workflows/ci.yml`
- **Trigger**: Pushes of `v*` tags (and `workflow_dispatch`)
- **Pipeline**: Install → Test → Build wheel/sdist → Publish to PyPI → Create GitHub Release
- **PyPI publishing**: Uses trusted publishing (`id-token: write`)
- **GitHub releases**: Uses `softprops/action-gh-release@v2` with auto-generated release notes

## Testing
- Tests require a GizmoSQL Docker container (auto-started by `tests/conftest.py` on port 31337)
- Ensure port 31337 is free before running tests
- Run: `pytest tests/`

## Dependencies (main)
- `dbt-core`, `dbt-common`, `dbt-adapters` — use `~=` (compatible release) pinning
- `adbc-driver-gizmosql` — use `>=` pinning; `pyarrow` is a transitive dep (don't pin separately)

## Dev Setup
```shell
python3 -m venv .venv && . .venv/bin/activate
pip install --editable .[dev]
```
