import pytest

from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowLimit,
    BaseShowSqlHeader,
)
from dbt.tests.util import run_dbt


class TestShowLimitGizmoSQL(BaseShowLimit):
    pass


class TestShowSqlHeaderGizmoSQL(BaseShowSqlHeader):
    pass


# Regression test for https://github.com/gizmodata/dbt-gizmosql/issues/6
#
# `dbt show -s <python_model>` used to crash with
#     AttributeError: 'NoneType' object has no attribute 'print_table'
# because the default `get_show_sql` wraps `compiled_code` in
# `SELECT * FROM (<compiled_code>) LIMIT N`, which produces invalid SQL for
# Python models (compiled_code is Python source, not SQL). The adapter
# execute then fails, leaving agate_table=None, which `task_end_messages`
# dereferences without checking.
#
# The fix: override `gizmosql__get_limit_sql` to detect Python models and
# select from the target relation directly, since a Python model is always
# materialized as a real table/view before `dbt show` runs.

PY_MODEL = """
def model(dbt, session):
    dbt.config(materialized="table")
    import pandas as pd
    return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
"""

SQL_UPSTREAM = """
select 1 as id, 'a' as name
union all
select 2, 'b'
union all
select 3, 'c'
"""

PY_MODEL_WITH_REF = """
def model(dbt, session):
    dbt.config(materialized="table")
    df = dbt.ref("upstream")
    return df
"""


class TestShowPythonModelGizmoSQL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": SQL_UPSTREAM,
            "py_model.py": PY_MODEL,
            "py_with_ref.py": PY_MODEL_WITH_REF,
        }

    def test_show_python_model(self, project):
        # Materialize everything first so the target tables exist.
        results = run_dbt(["run"])
        assert len(results) == 3

        # Standalone Python model — used to blow up with NoneType.
        preview = run_dbt(["show", "-s", "py_model"])
        assert preview is not None

        # Python model that calls dbt.ref — same path.
        preview = run_dbt(["show", "-s", "py_with_ref"])
        assert preview is not None

        # And a plain SQL model (regression guard — we mustn't break the
        # default path while fixing the Python one).
        preview = run_dbt(["show", "-s", "upstream"])
        assert preview is not None

    def test_show_python_model_with_limit(self, project):
        run_dbt(["run"])
        preview = run_dbt(["show", "-s", "py_model", "--limit", "2"])
        assert preview is not None
