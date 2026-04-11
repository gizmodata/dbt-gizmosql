# CLAUDE.md - Project Guide for Claude Code

## Project Overview
dbt-gizmosql is a [dbt](https://www.getdbt.com/) adapter for [GizmoSQL](https://gizmodata.com/gizmosql), an Apache Arrow Flight-based SQL engine with DuckDB and SQLite back-ends.

## Key Architecture
- **Driver**: Uses `adbc-driver-gizmosql` (not `adbc-driver-flightsql`) — the GizmoSQL driver wraps Flight SQL with a cleaner `connect()` API; supports OAuth/SSO via `auth_type="external"`
- **Connection**: `dbt/adapters/gizmosql/connections.py` — `GizmoSQLCredentials` and `GizmoSQLConnectionManager`
- **Adapter**: `dbt/adapters/gizmosql/impl.py` — `GizmoSQLAdapter`
- **Plugin entry**: `dbt/adapters/gizmosql/__init__.py`

## Release Checklist
**IMPORTANT**: Every release must complete all of the following steps. Do not push a `v*` tag (which triggers PyPI publish and GitHub Release via CI) until every item is done — treat this list as a hard release gate.

1. **Bump the version in FIVE places** (four code files + the changelog):
   1. `pyproject.toml` — `version = "x.y.z"`
   2. `dbt/adapters/gizmosql/__init__.py` — `__version__ = "x.y.z"`
   3. `dbt/adapters/gizmosql/__version__.py` — `version = "x.y.z"`
   4. `dbt/include/gizmosql/dbt_project.yml` — `version: x.y.z`
   5. `CHANGELOG.md` — **add a new `## vx.y.z (YYYY-MM-DD)` section at the top** describing user-visible changes. Group under `### Features`, `### Bug fixes`, `### Changes`, `### Dependency updates`, `### CI`, `### Test suite` as applicable. Reference issue numbers (`#6`) for bug fixes.
2. **Run the full test suite** (`pytest tests/`) — it must pass before you tag.
3. **Commit** — prefer a feature commit + a separate `Bump version to vx.y.z` commit (matching the existing git history pattern).
4. **Push master** — `git push origin master`.
5. **Tag and push the tag** — `git tag vx.y.z && git push origin vx.y.z`. The CI workflow is triggered by `v*` tag pushes and will build, publish to PyPI, and create the GitHub Release.
6. **Verify post-release** — check that the CI tag-run went green (`gh run list`), the GitHub Release was created (`gh release view vx.y.z`), and PyPI has the new version (`curl -sfo /dev/null -w '%{http_code}' https://pypi.org/pypi/dbt-gizmosql/x.y.z/json`).

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
