# dbt-gizmosql
An [dbt](https://www.getdbt.com/product/what-is-dbt) adapter for [GizmoSQL](https://gizmodata.com/gizmosql)

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

It has DuckDB and SQLite back-ends.  You can see more information about GizmoSQL [here](https://gizmodata.com/gizmosql).

### Installation

#### Option 1 - from PyPi
```shell
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

pip install --upgrade pip

python -m pip install dbt-core dbt-gizmosql
```

#### Option 2 - from source - for development of the adapter
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

### Usage of the dbt GizmoSQL adapter

#### Option 1: dbt init
You can setup the adapter by running `dbt init` - and choosing values that the wizard prompts you for.   

Then you can run by going into the directory for the project you just created:
```bash
dbt run
```

#### Option 2: Setup dbt.profiles.yml
Add something like the following to your `~/.dbt.profiles.yml` file (change the values to match your environment):
```yaml
my-gizmosql-db:
  target: dev
  outputs:
    dev:
      type: gizmosql
      host: localhost
      port: 31337
      user: [username]
      password: [password]
      use_encryption: True
      tls_skip_verify: True
      threads: 2
```

** Note **
### Adapter Scaffold default Versioning
This adapter plugin follows [semantic versioning](https://semver.org/). The version of this plugin is v1.11.x, in order to be compatible with dbt Core v1.11.x.

It's also brand new! For GizmoSQL-specific functionality, we will aim for backwards-compatibility wherever possible. We are likely to be iterating more quickly than most major-version-1 software projects. To that end, backwards-incompatible changes will be clearly communicated and limited to minor versions (once every three months).

## Join the dbt Community

- Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
- If one doesn't exist feel free to request a #db-GizmoSQL channel be made in the [#channel-requests](https://getdbt.slack.com/archives/C01D8J8AJDA) on dbt community slack channel.
- Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know on [Slack](http://community.getdbt.com/), or open [an issue](https://github.com/dbt-labs/dbt-redshift/issues/new)
- Want to help us build dbt? Check out the [Contributing Guide](https://github.com/dbt-labs/dbt/blob/HEAD/CONTRIBUTING.md)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).