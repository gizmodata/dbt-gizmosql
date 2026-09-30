"""dbt against a DuckLake catalog attached to the GizmoSQL server.

DuckLake doesn't support `DROP TABLE/VIEW ... CASCADE` ("Cascade Drop not
supported in DuckLake"), which dbt issues whenever it replaces an existing
relation. The adapter detects DuckLake catalogs via `duckdb_databases()` and
drops relations there without CASCADE.
"""

import pytest
from adbc_driver_gizmosql import dbapi as gizmosql_dbapi
from dbt.tests.util import get_connection, relation_from_name, run_dbt, run_dbt_and_capture

DUCKLAKE_CATALOG = "lake"


@pytest.fixture(scope="session")
def ducklake_catalog(gizmosql_server, tmp_path_factory):
    """Attach a DuckLake catalog (DuckDB metadata file + local data path) to
    the test server. ATTACH is server-wide, so dbt's sessions see it too."""
    lake_dir = tmp_path_factory.mktemp("ducklake")
    with gizmosql_dbapi.connect(
        uri=f"grpc://{gizmosql_server.host}:{gizmosql_server.port}",
        username=gizmosql_server.username,
        password=gizmosql_server.password,
    ) as conn, conn.cursor() as cursor:
        cursor.execute(
            operation=f"ATTACH IF NOT EXISTS 'ducklake:{lake_dir}/metadata.ducklake' "
            f"AS {DUCKLAKE_CATALOG} (DATA_PATH '{lake_dir}/data/')"
        )
    return DUCKLAKE_CATALOG


LAKE_SEED_CSV = """id,name
1,alice
2,bob
"""

LAKE_TABLE_SQL = """
{{ config(materialized='table') }}
select range::bigint as id from range(1, 6)
"""

LAKE_VIEW_SQL = """
{{ config(materialized='view') }}
select id * 10 as id_x10 from {{ ref('lake_table') }}
"""

PY_LAKE_TABLE_MODEL = """
import pyarrow as pa

def model(dbt, session):
    dbt.config(materialized="table")
    schema = pa.schema(fields=[("id", pa.int64())])
    batches = [pa.record_batch(data={"id": [1, 2, 3]}, schema=schema)]
    return pa.RecordBatchReader.from_batches(schema=schema, batches=batches)
"""

PY_LAKE_INCREMENTAL_MODEL = """
def model(dbt, session):
    dbt.config(materialized="incremental", incremental_strategy="append")
    df = dbt.ref("lake_table")
    if dbt.is_incremental:
        df = df.filter(df.id > 5)
    return df
"""


class TestDuckLake:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, gizmosql_server, ducklake_catalog):
        return {
            "type": "gizmosql",
            "threads": 1,
            "host": gizmosql_server.host,
            "port": gizmosql_server.port,
            "username": gizmosql_server.username,
            "password": gizmosql_server.password,
            "database": ducklake_catalog,
            "use_encryption": False,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"lake_seed.csv": LAKE_SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "lake_table.sql": LAKE_TABLE_SQL,
            "lake_view.sql": LAKE_VIEW_SQL,
            "lake_python.py": PY_LAKE_TABLE_MODEL,
            "lake_incremental.py": PY_LAKE_INCREMENTAL_MODEL,
        }

    def _count(self, project, name):
        relation = relation_from_name(adapter=project.adapter, name=name)
        return project.run_sql(sql=f"select count(*) from {relation}", fetch="one")[0]

    def test_catalog_type_detection(self, project, ducklake_catalog):
        with get_connection(adapter=project.adapter):
            assert project.adapter.get_catalog_type(database=ducklake_catalog) == "ducklake"
            assert project.adapter.get_catalog_type(database="LAKE") == "ducklake"
            assert project.adapter.get_catalog_type(database="dbt") == "duckdb"
            # Empty database -> the connection's current catalog (the profile's)
            assert project.adapter.get_catalog_type(database=None) == "ducklake"
            assert project.adapter.get_catalog_type(database="no_such_catalog") is None

    def test_models_rebuild_in_ducklake(self, project):
        # First run creates everything; the second replaces the existing
        # tables/views, which drops relations — the step that used CASCADE.
        for _ in range(2):
            results = run_dbt(args=["run"])
            assert len(results) == 4
            for r in results:
                assert r.status == "success", r.message

        assert self._count(project=project, name="lake_table") == 5
        assert self._count(project=project, name="lake_view") == 5
        assert self._count(project=project, name="lake_python") == 3
        assert self._count(project=project, name="lake_incremental") == 5

        # Re-seeding replaces the existing seed table
        for _ in range(2):
            results = run_dbt(args=["seed"])
            assert results[0].status == "success", results[0].message
        assert self._count(project=project, name="lake_seed") == 2

        # Full refresh drops and recreates the incremental model too
        results = run_dbt(args=["run", "--full-refresh", "--select", "lake_incremental"])
        assert results[0].status == "success", results[0].message
        assert self._count(project=project, name="lake_incremental") == 5


# ---- Contract constraints and indexes on DuckLake ---- #

LAKE_PARENT_SQL = """
{{ config(materialized='table') }}
select 1::bigint as id
"""

LAKE_CONTRACTED_SQL = """
{{ config(materialized='table') }}
select 1::bigint as id, 'a'::varchar as name, 5::integer as amount
"""

# Every constraint type dbt knows, plus an index. DuckLake supports only
# NOT NULL, so the rest must be skipped (with a warning) instead of failing.
LAKE_CONTRACTED_YML = """
models:
  - name: lake_contracted
    config:
      contract:
        enforced: true
      indexes:
        - columns: [name]
    constraints:
      - type: unique
        columns: [id, name]
      - type: foreign_key
        columns: [id]
        to: ref('lake_parent')
        to_columns: [id]
    columns:
      - name: id
        data_type: bigint
        constraints:
          - type: not_null
          - type: primary_key
      - name: name
        data_type: varchar
        constraints:
          - type: unique
            warn_unsupported: false
      - name: amount
        data_type: integer
        constraints:
          - type: check
            expression: "amount > 0"
"""


class TestDuckLakeConstraints:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, gizmosql_server, ducklake_catalog):
        return {
            "type": "gizmosql",
            "threads": 1,
            "host": gizmosql_server.host,
            "port": gizmosql_server.port,
            "username": gizmosql_server.username,
            "password": gizmosql_server.password,
            "database": ducklake_catalog,
            "use_encryption": False,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "lake_parent.sql": LAKE_PARENT_SQL,
            "lake_contracted.sql": LAKE_CONTRACTED_SQL,
            "schema.yml": LAKE_CONTRACTED_YML,
        }

    def test_unsupported_constraints_are_skipped(self, project):
        for _ in range(2):
            results, logs = run_dbt_and_capture(args=["run"])
            assert all(r.status == "success" for r in results), [r.message for r in results]

        # Unsupported constraints and indexes are skipped with a warning...
        for expected in (
            "skipping the primary_key constraint on column id",
            "skipping the check constraint on column amount",
            "skipping the unique constraint on columns id, name",
            "skipping the foreign_key constraint on columns id",
            "skipping the `indexes` config",
        ):
            assert expected in logs, expected
        # ...except where the constraint opts out of the warning
        assert "constraint on column name" not in logs

        # NOT NULL is supported by DuckLake, so it is still applied
        relation = relation_from_name(adapter=project.adapter, name="lake_contracted")
        nullability = project.run_sql(
            sql=f"""
                select column_name, is_nullable from information_schema.columns
                where table_catalog = '{relation.database}'
                  and table_schema = '{relation.schema}'
                  and table_name = '{relation.identifier}'
                order by ordinal_position
            """,
            fetch="all",
        )
        assert nullability == [("id", "NO"), ("name", "YES"), ("amount", "YES")]
