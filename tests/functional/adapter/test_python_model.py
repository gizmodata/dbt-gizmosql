import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)
from dbt.tests.util import get_connection, relation_from_name, run_dbt


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


# ---- Arrow RecordBatch / RecordBatchReader return types ---- #

PY_RECORD_BATCH_MODEL = """
import pyarrow as pa

def model(dbt, session):
    dbt.config(materialized="table")
    return pa.record_batch(data={"id": [1, 2, 3], "name": ["a", "b", "c"]})
"""

# Three batches streamed through a RecordBatchReader backed by a lazy
# generator — ingest must consume every batch, not just the first.
PY_RECORD_BATCH_READER_MODEL = """
import pyarrow as pa

def model(dbt, session):
    dbt.config(materialized="table")
    schema = pa.schema(fields=[("id", pa.int64()), ("name", pa.string())])

    def batches():
        for start in (0, 1000, 2000):
            ids = list(range(start, start + 1000))
            yield pa.record_batch(
                data={"id": ids, "name": [f"n{i}" for i in ids]}, schema=schema
            )

    return pa.RecordBatchReader.from_batches(schema=schema, batches=batches())
"""

# Returns a generator that transforms its upstream batch by batch. (dbt's
# parser requires `model()` itself to `return` exactly once, so the `yield`s
# live in an inner function.)
PY_GENERATOR_MODEL = """
import pyarrow as pa
import pyarrow.compute as pc

def model(dbt, session):
    dbt.config(materialized="table")
    upstream = dbt.ref("batch_upstream").to_arrow_table()

    def doubled_batches():
        for batch in upstream.to_batches(max_chunksize=2):
            ids = batch.column(i="id")
            yield pa.record_batch(data={"id": ids, "doubled": pc.multiply(ids, 2)})

    return doubled_batches()
"""

# A plain list of batches; the leading zero-row batch exercises the
# empty-batch peek.
PY_BATCH_LIST_MODEL = """
import pyarrow as pa

def model(dbt, session):
    dbt.config(materialized="table")
    return [
        pa.record_batch(data={"id": pa.array(obj=[], type=pa.int64())}),
        pa.record_batch(data={"id": [1, 2]}),
        pa.record_batch(data={"id": [3]}),
    ]
"""

# Any object implementing the Arrow PyCapsule stream protocol (e.g. a polars
# DataFrame) is streamed via `__arrow_c_stream__`.
PY_ARROW_STREAM_PROTOCOL_MODEL = """
import pyarrow as pa

class ArrowStream:
    def __init__(self, table):
        self._table = table

    def __arrow_c_stream__(self, requested_schema=None):
        return self._table.__arrow_c_stream__(requested_schema=requested_schema)

def model(dbt, session):
    dbt.config(materialized="table")
    return ArrowStream(table=pa.table(data={"id": [10, 20], "flag": [True, False]}))
"""

PY_EMPTY_READER_MODEL = """
import pyarrow as pa

def model(dbt, session):
    dbt.config(materialized="table")
    schema = pa.schema(fields=[("id", pa.int64()), ("name", pa.string())])
    return pa.RecordBatchReader.from_batches(schema=schema, batches=[])
"""

# Batches exist but none has rows — still creates the (empty) table.
PY_ZERO_ROW_BATCHES_MODEL = """
import pyarrow as pa

def model(dbt, session):
    dbt.config(materialized="table")
    empty = pa.record_batch(data={"id": pa.array(obj=[], type=pa.int64())})
    return [empty, empty]
"""

BATCH_UPSTREAM_SQL = """
select range::integer as id from range(1, 6)
"""


class TestPythonRecordBatches:
    """Python models may return `pa.RecordBatch`, `pa.RecordBatchReader`,
    iterables/generators of batches, or Arrow PyCapsule stream objects;
    streams are bulk-ingested to GizmoSQL batch by batch.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "batch_upstream.sql": BATCH_UPSTREAM_SQL,
            "single_batch.py": PY_RECORD_BATCH_MODEL,
            "batch_reader.py": PY_RECORD_BATCH_READER_MODEL,
            "batch_generator.py": PY_GENERATOR_MODEL,
            "batch_list.py": PY_BATCH_LIST_MODEL,
            "arrow_stream.py": PY_ARROW_STREAM_PROTOCOL_MODEL,
            "empty_reader.py": PY_EMPTY_READER_MODEL,
            "zero_row_batches.py": PY_ZERO_ROW_BATCHES_MODEL,
        }

    def _rows(self, project, name, columns):
        relation = relation_from_name(adapter=project.adapter, name=name)
        return project.run_sql(
            sql=f"select {columns} from {relation} order by 1", fetch="all"
        )

    def test_record_batch_returns(self, project):
        results = run_dbt(args=["run"])
        assert len(results) == 8
        for r in results:
            assert r.status == "success", r.message

        assert self._rows(project=project, name="single_batch", columns="id, name") == [
            (1, "a"),
            (2, "b"),
            (3, "c"),
        ]

        assert self._rows(
            project=project, name="batch_reader", columns="count(*), min(id), max(id)"
        ) == [(3000, 0, 2999)]

        assert self._rows(
            project=project, name="batch_generator", columns="id, doubled"
        ) == [(i, i * 2) for i in range(1, 6)]

        assert self._rows(project=project, name="batch_list", columns="id") == [
            (1,),
            (2,),
            (3,),
        ]

        assert self._rows(project=project, name="arrow_stream", columns="id, flag") == [
            (10, True),
            (20, False),
        ]

        for name, expected_columns in (
            ("empty_reader", [("id", "BIGINT"), ("name", "VARCHAR")]),
            ("zero_row_batches", [("id", "BIGINT")]),
        ):
            empty = relation_from_name(adapter=project.adapter, name=name)
            count = project.run_sql(sql=f"select count(*) from {empty}", fetch="one")
            assert count[0] == 0, name
            with get_connection(adapter=project.adapter):
                columns = project.adapter.get_columns_in_relation(relation=empty)
            assert [(c.name, c.dtype.upper()) for c in columns] == expected_columns, name


INCREMENTAL_UPSTREAM_SQL = """
{{ config(materialized='table') }}
select range::bigint as id, 'v1' as version from range(1, 6)
"""

# Mirrors dbt's BasePythonIncrementalTests, but streams its result as a
# generator of record batches instead of returning a relation.
PY_INCREMENTAL_BATCHES_MODEL = """
import pyarrow.compute as pc

def model(dbt, session):
    dbt.config(materialized="incremental", unique_key="id")
    upstream = dbt.ref("incremental_upstream").to_arrow_table()
    is_incremental = dbt.is_incremental

    def batches():
        for batch in upstream.to_batches(max_chunksize=2):
            if is_incremental:
                # incremental runs should only apply to part of the data
                batch = batch.filter(mask=pc.greater(batch.column(i="id"), 5))
            yield batch

    return batches()
"""


class TestPythonIncrementalRecordBatches:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_upstream.sql": INCREMENTAL_UPSTREAM_SQL,
            "incremental_batches.py": PY_INCREMENTAL_BATCHES_MODEL,
        }

    def _rows(self, project):
        relation = relation_from_name(adapter=project.adapter, name="incremental_batches")
        return project.run_sql(
            sql=f"select id, version from {relation} order by id", fetch="all"
        )

    def test_incremental_record_batches(self, project):
        # First run creates the table from all upstream batches
        results = run_dbt(args=["run"])
        assert all(r.status == "success" for r in results), [r.message for r in results]
        assert self._rows(project=project) == [(i, "v1") for i in range(1, 6)]

        # Re-running with no new upstream rows streams only zero-row batches
        run_dbt(args=["run", "--select", "incremental_batches"])
        assert self._rows(project=project) == [(i, "v1") for i in range(1, 6)]

        # New upstream rows — one filtered out (id 0), two appended (6, 7)
        upstream = relation_from_name(adapter=project.adapter, name="incremental_upstream")
        project.run_sql(
            sql=f"insert into {upstream} (id, version) values (0, 'v2'), (6, 'v2'), (7, 'v2')"
        )
        run_dbt(args=["run", "--select", "incremental_batches"])
        assert self._rows(project=project) == [(i, "v1") for i in range(1, 6)] + [
            (6, "v2"),
            (7, "v2"),
        ]


# ---- Streaming from an external ADBC source (e.g. Db2) ---- #

# Mirrors a real-world model that extracts from Db2 via adbc-driver-db2 in a
# helper function. Here the "external source" is the test GizmoSQL server,
# reached over its own ADBC connection.
PY_SOURCE_CONNECT = """
from adbc_driver_gizmosql import dbapi as gizmosql

def connect_to_source(dbt):
    return gizmosql.connect(
        uri=dbt.config.meta_get("source_uri"),
        username=dbt.config.meta_get("source_username"),
        password=dbt.config.meta_get("source_password"),
    )
"""

SOURCE_ROWS_SQL = """
{{ config(materialized='table') }}
select range::bigint as id from range(1, 6)
"""

# Anti-pattern: leaving the `with` block closes the cursor and connection, so
# the returned reader is already closed when dbt-gizmosql streams it.
PY_CLOSED_SOURCE_READER_MODEL = PY_SOURCE_CONNECT + """
def extract(dbt):
    with connect_to_source(dbt=dbt) as conn, conn.cursor() as cursor:
        cursor.execute(operation=f"select id from {dbt.this.database}.{dbt.this.schema}.source_rows")
        return cursor.fetch_record_batch()

def model(dbt, session):
    dbt.config(materialized="table")
    return extract(dbt=dbt)
"""

# Fix: `yield` the reader instead of returning it. The source stays open
# while dbt-gizmosql streams the reader, then the `with` block closes it. An
# incremental run with no new rows yields a reader with zero batches, whose
# schema still creates the (empty) temp table.
PY_INCREMENTAL_SOURCE_MODEL = PY_SOURCE_CONNECT + """
def extract(dbt):
    query = f"select id from {dbt.this.database}.{dbt.this.schema}.source_rows"
    if dbt.is_incremental:
        query += f" where id > (select max(id) from {dbt.this})"
    with connect_to_source(dbt=dbt) as conn, conn.cursor() as cursor:
        cursor.execute(operation=query)
        yield cursor.fetch_record_batch()

def model(dbt, session):
    dbt.config(materialized="incremental", incremental_strategy="append")
    return extract(dbt=dbt)
"""


class TestPythonStreamFromExternalSource:
    @pytest.fixture(scope="class")
    def project_config_update(self, gizmosql_server):
        return {
            "models": {
                "+meta": {
                    "source_uri": f"grpc://{gizmosql_server.host}:{gizmosql_server.port}",
                    "source_username": gizmosql_server.username,
                    "source_password": gizmosql_server.password,
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_rows.sql": SOURCE_ROWS_SQL,
            "closed_source_reader.py": PY_CLOSED_SOURCE_READER_MODEL,
            "incremental_source.py": PY_INCREMENTAL_SOURCE_MODEL,
        }

    def _ids(self, project):
        relation = relation_from_name(adapter=project.adapter, name="incremental_source")
        rows = project.run_sql(sql=f"select id from {relation} order by id", fetch="all")
        return [row[0] for row in rows]

    def test_yielded_reader_incremental(self, project):
        # The source isn't a dbt dependency of the model (as with a real Db2
        # source), so build it first.
        run_dbt(args=["run", "--select", "source_rows"])
        results = run_dbt(args=["run", "--select", "incremental_source"])
        assert results[0].status == "success", results[0].message
        assert self._ids(project=project) == [1, 2, 3, 4, 5]

        # No new source rows: the reader has zero batches — still succeeds
        results = run_dbt(args=["run", "--select", "incremental_source"])
        assert results[0].status == "success", results[0].message
        assert self._ids(project=project) == [1, 2, 3, 4, 5]

        # New source rows are appended
        source = relation_from_name(adapter=project.adapter, name="source_rows")
        project.run_sql(sql=f"insert into {source} (id) values (6), (7)")
        results = run_dbt(args=["run", "--select", "incremental_source"])
        assert results[0].status == "success", results[0].message
        assert self._ids(project=project) == [1, 2, 3, 4, 5, 6, 7]

    def test_closed_reader_gets_actionable_error(self, project):
        run_dbt(args=["run", "--select", "source_rows"])
        results = run_dbt(args=["run", "--select", "closed_source_reader"], expect_pass=False)
        assert results[0].status == "error"
        assert "record batch stream that was already closed" in results[0].message
        assert "yield cursor.fetch_record_batch()" in results[0].message
