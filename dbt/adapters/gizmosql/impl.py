import os
from typing import Dict, List, Optional

import duckdb
import pyarrow as pa

from dbt.adapters.base.meta import available
from dbt.adapters.sql import SQLAdapter as adapter_cls

from dbt.adapters.gizmosql import GizmoSQLConnectionManager
from dbt.adapters.gizmosql.column import DuckDBColumn
from dbt.adapters.gizmosql.relation import GizmoSQLRelation


class GizmoSQLAdapter(adapter_cls):
    """
    Controls actual implementation of adapter, and ability to override certain methods.
    """

    Relation = GizmoSQLRelation
    ConnectionManager = GizmoSQLConnectionManager

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

    @available
    def load_seed_from_csv(
        self,
        table_name: str,
        csv_file_path: str,
        db_schema: str,
        column_types: Optional[Dict[str, str]] = None,
        delimiter: Optional[str] = None,
    ) -> int:
        """Load a seed CSV using DuckDB (client-side) for type inference,
        then bulk-ingest the Arrow data to GizmoSQL via ADBC.

        This avoids the standard dbt batch-INSERT path which loses NULL
        semantics and type information when going through Flight SQL
        parameter binding.
        """
        # Read CSV with DuckDB for correct null handling and type inference
        local_db = duckdb.connect(":memory:")
        try:
            opts = "auto_detect=true, all_varchar=false"
            if delimiter:
                escaped = delimiter.replace("'", "''")
                opts += f", delim='{escaped}'"
            arrow_table = local_db.execute(
                f"SELECT * FROM read_csv('{csv_file_path}', {opts})"
            ).to_arrow_table()
        finally:
            local_db.close()

        # Handle empty CSV (header only, no data rows)
        if arrow_table.num_rows == 0:
            # Create table from schema, insert nothing
            connection = self.connections.get_thread_connection()
            col_defs = ", ".join(
                f'"{f.name}" {self._arrow_to_duckdb_type(f.type)}'
                for f in arrow_table.schema
            )
            cursor = connection.handle.cursor()
            try:
                cursor.execute(
                    f'CREATE TABLE "{db_schema}"."{table_name}" ({col_defs})'
                )
            finally:
                cursor.close()
            return 0

        # Apply column_types overrides from dbt config if specified
        if column_types:
            type_map = {
                "text": pa.string(), "varchar": pa.string(), "string": pa.string(),
                "integer": pa.int32(), "int": pa.int32(), "bigint": pa.int64(),
                "float": pa.float64(), "double": pa.float64(), "numeric": pa.float64(),
                "boolean": pa.bool_(), "bool": pa.bool_(),
                "date": pa.date32(), "timestamp": pa.timestamp("us"),
            }
            new_fields = []
            for field in arrow_table.schema:
                override = column_types.get(field.name)
                if override and override.lower() in type_map:
                    new_fields.append(pa.field(field.name, type_map[override.lower()]))
                else:
                    new_fields.append(field)
            new_schema = pa.schema(new_fields)
            arrow_table = arrow_table.cast(new_schema)

        # Bulk ingest via ADBC — creates the table with Arrow-inferred types
        connection = self.connections.get_thread_connection()
        cursor = connection.handle.cursor()
        try:
            rows = cursor.adbc_ingest(
                table_name,
                arrow_table,
                mode="create",
                db_schema_name=db_schema,
            )
        finally:
            cursor.close()

        return rows

    @staticmethod
    def _arrow_to_duckdb_type(arrow_type) -> str:
        """Map Arrow types to DuckDB SQL type names."""
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "VARCHAR"
        if pa.types.is_int32(arrow_type):
            return "INTEGER"
        if pa.types.is_int64(arrow_type):
            return "BIGINT"
        if pa.types.is_float64(arrow_type):
            return "DOUBLE"
        if pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        if pa.types.is_date(arrow_type):
            return "DATE"
        if pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        return "VARCHAR"

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> List[DuckDBColumn]:
        """Get a list of the column names and data types from the given sql.

        :param str sql: The sql to execute.
        :return: List[DuckDBColumn]
        """
        # Taking advantage of yet another amazing DuckDB SQL feature right here: the
        # ability to DESCRIBE a query instead of a relation
        describe_sql = f"DESCRIBE ({sql})"
        _, cursor = self.connections.add_select_query(describe_sql)
        flattened_columns = []
        for row in cursor.fetchall():
            name, dtype = row[0], row[1]
            column = DuckDBColumn(column=name, dtype=dtype)
            flattened_columns.extend(column.flatten())
        cursor.close()
        return flattened_columns
