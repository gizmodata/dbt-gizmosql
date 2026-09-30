"""`duckdb_column_definitions` creates tables for empty results (which ADBC
bulk ingest can't) with the same column types a non-empty ingest would."""

import duckdb
import pyarrow as pa

from dbt.adapters.gizmosql.impl import duckdb_column_definitions


def test_maps_arrow_types_to_duckdb_types():
    schema = pa.schema(
        fields=[
            ("id", pa.int64()),
            ("name", pa.string()),
            ("amount", pa.decimal128(12, 2)),
            ("created_at", pa.timestamp("us", tz="UTC")),
            ("tags", pa.list_(pa.string())),
            ("attrs", pa.struct([("a", pa.int32())])),
        ]
    )
    assert duckdb_column_definitions(schema=schema) == (
        '"id" BIGINT, "name" VARCHAR, "amount" DECIMAL(12,2), '
        '"created_at" TIMESTAMP WITH TIME ZONE, "tags" VARCHAR[], "attrs" STRUCT(a INTEGER)'
    )


def test_quotes_column_names():
    schema = pa.schema(fields=[('odd "name"', pa.int32()), ("select", pa.bool_())])
    assert duckdb_column_definitions(schema=schema) == '"odd ""name""" INTEGER, "select" BOOLEAN'


def test_definitions_are_valid_ddl():
    schema = pa.schema(
        fields=[("amount", pa.decimal128(12, 2)), ("m", pa.map_(pa.string(), pa.int64()))]
    )
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(query=f"create table t ({duckdb_column_definitions(schema=schema)})")
        rows = con.execute(
            query="select column_name, data_type from information_schema.columns "
            "where table_name = 't' order by ordinal_position"
        ).fetchall()
    finally:
        con.close()
    assert rows == [("amount", "DECIMAL(12,2)"), ("m", "MAP(VARCHAR, BIGINT)")]
