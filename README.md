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
This repo contains the base code to help you start to build out your dbt-gizmosql adapter plugin, for more information on how to build out the adapter please follow the [docs](https://docs.getdbt.com/docs/contributing/building-a-new-adapter)

** Note ** this `README` is meant to be replaced with what information would be required to use your adpater once your at a point todo so.

** Note **
### Adapter Scaffold default Versioning
This adapter plugin follows [semantic versioning](https://semver.org/). The first version of this plugin is v1.10.0, in order to be compatible with dbt Core v1.10.0.

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