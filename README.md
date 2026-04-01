# dbt-gizmosql
A [dbt](https://www.getdbt.com/product/what-is-dbt) adapter for [GizmoSQL](https://gizmodata.com/gizmosql)

[<img src="https://img.shields.io/badge/GitHub-gizmodata%2Fdbt--gizmosql-blue.svg?logo=Github">](https://github.com/gizmodata/dbt-gizmosql)
[<img src="https://img.shields.io/badge/GitHub-gizmodata%2Fgizmosql--public-blue.svg?logo=Github">](https://github.com/gizmodata/gizmosql-public)
[![dbt-gizmosql-ci](https://github.com/gizmodata/dbt-gizmosql/actions/workflows/ci.yml/badge.svg)](https://github.com/gizmodata/dbt-gizmosql/actions/workflows/ci.yml)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/dbt-gizmosql)](https://pypi.org/project/dbt-gizmosql/)
[![PyPI version](https://badge.fury.io/py/dbt-gizmosql.svg)](https://badge.fury.io/py/dbt-gizmosql)
[![PyPI Downloads](https://img.shields.io/pypi/dm/dbt-gizmosql.svg)](https://pypi.org/project/dbt-gizmosql/)

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## GizmoSQL
GizmoSQL is an Apache Arrow Flight-based SQL engine for data warehouses. It is designed to be fast, scalable, and easy to use.

It has DuckDB and SQLite back-ends. You can see more information about GizmoSQL [here](https://gizmodata.com/gizmosql).

## Features

This adapter provides feature parity with [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) for all features applicable to a remote Flight SQL connection:

### Materializations
- **Table** and **View** (SQL and Python)
- **Incremental** with four strategies:
  - `append` -- simple record additions
  - `delete+insert` -- key-based upserts with DuckDB's `DELETE...USING` syntax
  - `merge` -- uses DuckDB's `MERGE` with `UPDATE BY NAME` / `INSERT BY NAME`
  - `microbatch` -- time-based batch processing via `event_time` windows
- **Snapshot** (check and timestamp modes) using UPDATE+INSERT pattern
- **Schema change handling**: `ignore`, `append_new_columns`, `sync_all_columns`, `fail`

### Python Models
Python models execute client-side using a local DuckDB instance for full API compatibility, then ship results to GizmoSQL via ADBC bulk ingest:

```python
def model(dbt, session):
    dbt.config(materialized="table")
    df = dbt.ref("upstream_model")
    df = df.filter(df.amount > 100)
    return df
```

- Supports DuckDB relations, pandas DataFrames, and PyArrow Tables as return types
- `dbt.ref()` and `dbt.source()` fetch data from GizmoSQL as Arrow and expose it as DuckDB relations
- Incremental Python models supported (with proper `dbt.is_incremental` handling)

### Seed Loading
Seeds are loaded using DuckDB's CSV reader on the client side with ADBC bulk ingest to the server:

- Correct null handling (empty CSV fields become SQL `NULL`, not the string `'null'`)
- Proper type inference (dates detected as `DATE`, integers as `BIGINT`, etc.)
- Supports `column_types` overrides and custom delimiters
- Significantly faster than dbt's default batch `INSERT` path

### Constraints
All constraint types are enforced: `CHECK`, `NOT NULL`, `UNIQUE`, `PRIMARY KEY`, `FOREIGN KEY`.

### Documentation
- `persist_docs` support (`COMMENT ON` for relations and columns)
- Full catalog generation with `dbt docs generate`

### Utility Macros
DuckDB-compatible overrides for: `dateadd`, `last_day`, `listagg`, `split_part`.

## Installation

### Option 1 - from PyPi
```shell
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

pip install --upgrade pip

python -m pip install dbt-core dbt-gizmosql
```

### Option 2 - from source (for development)
```shell
git clone https://github.com/gizmodata/dbt-gizmosql

cd dbt-gizmosql

# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install the dbt GizmoSQL adapter - in editable mode with dev dependencies
pip install --editable .[dev]
```

## Configuration

### Profile setup

Add the following to your `~/.dbt/profiles.yml` (change values to match your environment):

```yaml
my-gizmosql-db:
  target: dev
  outputs:
    dev:
      type: gizmosql
      host: localhost
      port: 31337
      database: dbt
      user: [username]
      password: [password]
      use_encryption: True
      tls_skip_verify: True
      threads: 2
```

### OAuth/SSO Authentication
For browser-based OAuth/SSO, use `auth_type: external` -- no username or password needed:
```yaml
my-gizmosql-db:
  target: dev
  outputs:
    dev:
      type: gizmosql
      host: gizmosql.example.com
      port: 31337
      auth_type: external
      use_encryption: True
      threads: 2
```

## Architecture

This adapter connects to GizmoSQL via Apache Arrow Flight SQL using the [ADBC](https://arrow.apache.org/adbc/) driver (`adbc-driver-gizmosql`). Key architectural decisions:

- **Autocommit mode**: Each statement auto-commits immediately. Flight SQL's `PREPARE` phase validates against committed catalog state, so explicit transactions would cause DDL from earlier statements to be invisible to later ones.
- **Client-side DuckDB**: Seeds and Python models use a local DuckDB instance for processing, with results shipped to the server via ADBC bulk ingest (Arrow columnar format over gRPC).
- **MERGE BY NAME**: Incremental merges use DuckDB's `UPDATE BY NAME` / `INSERT BY NAME` syntax, which is resilient to column ordering differences.

## Versioning

This adapter follows [semantic versioning](https://semver.org/). The major.minor version tracks dbt-core (e.g., dbt-core 1.11.x -> dbt-gizmosql 1.11.x).

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Open [an issue](https://github.com/gizmodata/dbt-gizmosql/issues)
- Want to contribute? Pull requests are welcome

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
