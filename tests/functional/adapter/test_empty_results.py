"""Empty results create tables with real column types, not all VARCHAR.

ADBC bulk ingest can't create a table from zero rows, so the adapter creates
it from the result's schema. An incremental model whose first load is empty
must still get correctly typed columns, or every later append is cast to
text.
"""

import pytest
from dbt.tests.util import get_connection, relation_from_name, run_dbt

TYPED_UPSTREAM_SQL = """
{{ config(materialized='table') }}
select range::bigint as id, (range * 1.5)::decimal(10,2) as amount, 'x' || range as label
from range(1, 4)
"""

# First (non-incremental) load filters everything out, so the target is
# created from an empty result; the incremental run then appends real rows.
PY_EMPTY_FIRST_LOAD_MODEL = """
def model(dbt, session):
    dbt.config(materialized="incremental")
    df = dbt.ref("typed_upstream")
    if not dbt.is_incremental:
        df = df.filter("id > 100")
    return df
"""

EMPTY_SEED_CSV = "id,name,joined\n"

SEEDS_YML = """
seeds:
  - name: empty_seed
    config:
      column_types:
        id: bigint
        joined: date
"""


class TestEmptyResultsAreTyped:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "typed_upstream.sql": TYPED_UPSTREAM_SQL,
            "empty_first_load.py": PY_EMPTY_FIRST_LOAD_MODEL,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"empty_seed.csv": EMPTY_SEED_CSV, "seeds.yml": SEEDS_YML}

    def _column_types(self, project, name):
        relation = relation_from_name(adapter=project.adapter, name=name)
        with get_connection(adapter=project.adapter):
            columns = project.adapter.get_columns_in_relation(relation=relation)
        return [(c.name, c.dtype.upper()) for c in columns]

    def test_incremental_model_with_empty_first_load(self, project):
        results = run_dbt(args=["run"])
        assert all(r.status == "success" for r in results), [r.message for r in results]
        relation = relation_from_name(adapter=project.adapter, name="empty_first_load")
        assert project.run_sql(sql=f"select count(*) from {relation}", fetch="one")[0] == 0
        expected_types = [("id", "BIGINT"), ("amount", "DECIMAL(10,2)"), ("label", "VARCHAR")]
        assert self._column_types(project=project, name="empty_first_load") == expected_types

        results = run_dbt(args=["run", "--select", "empty_first_load"])
        assert results[0].status == "success", results[0].message
        assert self._column_types(project=project, name="empty_first_load") == expected_types
        rows = project.run_sql(
            sql=f"select id, amount, label from {relation} order by id", fetch="all"
        )
        assert [(r[0], float(r[1]), r[2]) for r in rows] == [
            (1, 1.5, "x1"),
            (2, 3.0, "x2"),
            (3, 4.5, "x3"),
        ]

    def test_empty_seed_honors_column_types(self, project):
        # dbt skips loading rows for an empty seed, so the table must still be
        # created — on the first seed and when an existing table is reset.
        for _ in range(2):
            results = run_dbt(args=["seed"])
            assert results[0].status == "success", results[0].message
            assert self._column_types(project=project, name="empty_seed") == [
                ("id", "BIGINT"),
                ("name", "VARCHAR"),
                ("joined", "DATE"),
            ]
