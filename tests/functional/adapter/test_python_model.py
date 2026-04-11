import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)
from dbt.tests.util import relation_from_name, run_dbt


class TestPythonModelGizmoSQL(BasePythonModelTests):
    pass


class TestPythonIncrementalGizmoSQL(BasePythonIncrementalTests):
    pass


# ---- `session.remote_sql()` — server-side pushdown from a Python model ---- #

BIG_UPSTREAM_SQL = """
select 1 as id, 'alice' as name, 10 as amount
union all
select 2 as id, 'bob'   as name, 20 as amount
union all
select 3 as id, 'joe'   as name, 30 as amount
union all
select 4 as id, 'joe'   as name, 40 as amount
union all
select 5 as id, 'carol' as name, 50 as amount
"""

# Exactly the shape the user asked about: instead of
#   df = dbt.ref('big_upstream').filter("name = 'joe'")
# which pulls the entire table over the network, use
#   session.remote_sql("select * from ... where name = 'joe'")
# so the filter runs on the GizmoSQL server.
PY_REMOTE_SQL_MODEL = """
def model(dbt, session):
    dbt.config(materialized="table")
    schema = dbt.this.schema
    return session.remote_sql(
        f"select id, name, amount from {schema}.big_upstream where name = 'joe'"
    )
"""


class TestPythonRemoteSQLPushdown:
    """Proves `session.remote_sql(query)` runs on the GizmoSQL server and
    returns a chainable local relation — the filter is applied server-side,
    only the matching rows cross the wire, and the Python model materializes
    exactly those rows.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "big_upstream.sql": BIG_UPSTREAM_SQL,
            "joe_rows.py": PY_REMOTE_SQL_MODEL,
        }

    def test_remote_sql_filters_server_side(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        for r in results:
            assert r.status == "success", r.message

        joe_rows = relation_from_name(project.adapter, "joe_rows")

        n = project.run_sql(f"select count(*) from {joe_rows}", fetch="one")
        assert n[0] == 2, f"expected 2 'joe' rows, got {n[0]}"

        rows = project.run_sql(
            f"select id, name, amount from {joe_rows} order by id",
            fetch="all",
        )
        assert rows == [(3, "joe", 30), (4, "joe", 40)]
