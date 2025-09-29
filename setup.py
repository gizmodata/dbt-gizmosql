#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-gizmosql-adapter"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.7.0"
description = """The GizmoSQL adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Philip Moore",
    author_email="philip@gizmodata.com",
    url="https://github.com/gizmodata/dbt-gizmosql-adapter.git",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.10.0",
        "dbt-common~=1.32.0",
        "dbt-adapters~=1.16.0",
        "adbc-driver-flightsql~=1.8.0",
        "pyarrow==21.0.*"
    ],
)
