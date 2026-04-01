import pytest

from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch


# Override input model to use UTC timestamps (no timezone offset)
# GizmoSQL/DuckDB requires ICU extension for non-UTC timestamps
_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 00:00:00' as event_time
union all
select 2 as id, TIMESTAMP '2020-01-02 00:00:00' as event_time
union all
select 3 as id, TIMESTAMP '2020-01-03 00:00:00' as event_time
"""


class TestMicrobatchGizmoSQL(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def input_model_sql(self):
        return _input_model_sql

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project):
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return (
            f"INSERT INTO {test_schema_relation}.input_model (id, event_time) "
            f"VALUES (4, TIMESTAMP '2020-01-04 00:00:00'), "
            f"(5, TIMESTAMP '2020-01-05 00:00:00')"
        )
