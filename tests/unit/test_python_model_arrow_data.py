"""Unit tests for `_to_arrow_data`, which normalizes a Python model's return
value into the Arrow data handed to ADBC bulk ingest. No server needed."""

import duckdb
import pandas as pd
import pyarrow as pa
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.gizmosql.impl import _DuckDBDataFrame, _to_arrow_data

SCHEMA = pa.schema(fields=[("id", pa.int64())])


def _batch(ids):
    return pa.record_batch(data={"id": pa.array(obj=ids, type=pa.int64())})


class _ArrowStream:
    """Minimal Arrow PyCapsule stream producer (stand-in for e.g. polars)."""

    def __init__(self, table):
        self._table = table

    def __arrow_c_stream__(self, requested_schema=None):
        return self._table.__arrow_c_stream__(requested_schema=requested_schema)


# ---- Table-like results are materialized as a pa.Table ---- #


@pytest.mark.parametrize(
    "make_df",
    [
        pytest.param(lambda: pa.table(data={"id": [1, 2]}), id="table"),
        pytest.param(lambda: _batch(ids=[1, 2]), id="record_batch"),
        pytest.param(lambda: pd.DataFrame(data={"id": [1, 2]}), id="pandas"),
    ],
)
def test_table_like_results_become_tables(make_df):
    result = _to_arrow_data(df=make_df())
    assert isinstance(result, pa.Table)
    assert result.column(i="id").to_pylist() == [1, 2]


def test_duckdb_relations_become_tables():
    con = duckdb.connect(database=":memory:")
    try:
        relation = con.sql(query="select 1 as id union all select 2")
        for df in (relation, _DuckDBDataFrame(relation)):
            result = _to_arrow_data(df=df)
            assert isinstance(result, pa.Table)
            assert sorted(result.column(i="id").to_pylist()) == [1, 2]
    finally:
        con.close()


def test_pandas_index_is_not_ingested():
    df = pd.DataFrame(data={"id": [1, 2]}, index=["a", "b"])
    assert _to_arrow_data(df=df).column_names == ["id"]


# ---- Streams are passed through as a RecordBatchReader ---- #


@pytest.mark.parametrize(
    "make_df",
    [
        pytest.param(
            lambda: pa.RecordBatchReader.from_batches(
                schema=SCHEMA, batches=[_batch(ids=[1]), _batch(ids=[2, 3])]
            ),
            id="reader",
        ),
        pytest.param(lambda: [_batch(ids=[1]), _batch(ids=[2, 3])], id="list"),
        pytest.param(
            lambda: (_batch(ids=ids) for ids in ([1], [2, 3])), id="generator"
        ),
        pytest.param(
            lambda: _ArrowStream(table=pa.table(data={"id": [1, 2, 3]})),
            id="arrow_c_stream",
        ),
    ],
)
def test_streams_become_readers(make_df):
    result = _to_arrow_data(df=make_df())
    assert isinstance(result, pa.RecordBatchReader)
    assert result.read_all().column(i="id").to_pylist() == [1, 2, 3]


def test_generator_is_consumed_lazily():
    """Only the batches needed to detect a non-empty stream are read up front;
    the rest are pulled by the ingest as it streams."""
    produced = []

    def batches():
        for ids in ([], [1], [2], [3]):
            produced.append(ids)
            yield _batch(ids=ids)

    result = _to_arrow_data(df=batches())
    assert isinstance(result, pa.RecordBatchReader)
    # Peeked past the zero-row batch to the first non-empty one — no further.
    assert produced == [[], [1]]

    assert result.read_all().column(i="id").to_pylist() == [1, 2, 3]
    assert produced == [[], [1], [2], [3]]


# ---- Streams without rows become an empty table (ingest can't create from them) ---- #


@pytest.mark.parametrize(
    "make_df",
    [
        pytest.param(
            lambda: pa.RecordBatchReader.from_batches(schema=SCHEMA, batches=[]),
            id="reader_no_batches",
        ),
        pytest.param(
            lambda: pa.RecordBatchReader.from_batches(
                schema=SCHEMA, batches=[_batch(ids=[]), _batch(ids=[])]
            ),
            id="reader_zero_row_batches",
        ),
        pytest.param(lambda: [_batch(ids=[]), _batch(ids=[])], id="list_zero_row_batches"),
        pytest.param(
            lambda: _ArrowStream(table=SCHEMA.empty_table()), id="arrow_c_stream_empty"
        ),
    ],
)
def test_streams_without_rows_become_empty_tables(make_df):
    result = _to_arrow_data(df=make_df())
    assert isinstance(result, pa.Table)
    assert result.num_rows == 0
    assert result.schema == SCHEMA


# ---- Iterables that can't be ingested raise a clear error ---- #


@pytest.mark.parametrize(
    "make_df, type_name",
    [
        pytest.param(lambda: [], "NoneType", id="empty_list"),
        pytest.param(lambda: iter(()), "NoneType", id="empty_generator"),
        pytest.param(
            lambda: [pa.table(data={"id": [1]})], "Table", id="list_of_tables"
        ),
        pytest.param(lambda: [{"id": 1}], "dict", id="list_of_dicts"),
    ],
)
def test_invalid_iterables_raise(make_df, type_name):
    with pytest.raises(DbtRuntimeError, match=f"first item is {type_name}, expected pyarrow.RecordBatch"):
        _to_arrow_data(df=make_df())
