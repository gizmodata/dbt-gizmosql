import importlib.util
import os
import tempfile
import traceback
from typing import Dict, List, Optional

import duckdb
import pyarrow as pa

from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.sql import SQLAdapter as adapter_cls
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.gizmosql import GizmoSQLConnectionManager
from dbt.adapters.gizmosql.column import DuckDBColumn
from dbt.adapters.gizmosql.relation import GizmoSQLRelation


class GizmoSQLAdapter(adapter_cls):
    """
    Controls actual implementation of adapter, and ability to override certain methods.
    """

    Relation = GizmoSQLRelation
    ConnectionManager = GizmoSQLConnectionManager

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

    def valid_incremental_strategies(self):
        return ["append", "delete+insert", "merge"]

    @classmethod
    def render_column_constraint(cls, constraint) -> Optional[str]:
        """Override to strip database prefix from FK references.

        DuckDB rejects FOREIGN KEY references with 3-part qualified names
        (e.g. "db"."schema"."table") as cross-database, even when all
        objects are in the same database. Strip the catalog prefix.
        """
        rendered = super().render_column_constraint(constraint)
        if rendered and constraint.to and constraint.type.value == "foreign_key":
            # Strip leading "catalog". from the reference
            parts = constraint.to.split(".")
            if len(parts) == 3:
                schema_table = ".".join(parts[1:])
                rendered = rendered.replace(constraint.to, schema_table)
        return rendered

    @classmethod
    def render_model_constraint(cls, constraint) -> Optional[str]:
        """Override to strip database prefix from FK references."""
        rendered = super().render_model_constraint(constraint)
        if rendered and hasattr(constraint, "to") and constraint.to and constraint.type.value == "foreign_key":
            parts = constraint.to.split(".")
            if len(parts) == 3:
                schema_table = ".".join(parts[1:])
                rendered = rendered.replace(constraint.to, schema_table)
        return rendered

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

    def submit_python_job(self, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        """Execute a Python model client-side using local DuckDB.

        Flow:
        1. Fetch ref/source data from GizmoSQL as Arrow tables
        2. Register them in a local DuckDB instance
        3. Run the Python model against local DuckDB
        4. Ship the result back to GizmoSQL via ADBC ingest
        """
        connection = self.connections.get_thread_connection()
        adbc_conn = connection.handle

        # Create a local DuckDB for model execution
        local_db = duckdb.connect(":memory:")

        # The load_df_function fetches data from GizmoSQL and registers locally
        def load_df_function(table_name):
            cursor = adbc_conn.cursor()
            try:
                cursor.execute(f"SELECT * FROM {table_name}")
                arrow_table = cursor.fetch_arrow_table()
            finally:
                cursor.close()
            # Register in local DuckDB so the model can query it
            local_db.register(table_name.replace('"', ''), arrow_table)
            return local_db.query(f"SELECT * FROM '{table_name.replace(chr(34), '')}'")

        # Write compiled code to a temp file and load as module
        mod_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
        try:
            mod_file.write(compiled_code.lstrip().encode("utf-8"))
            mod_file.close()

            identifier = parsed_model["alias"]
            spec = importlib.util.spec_from_file_location(identifier, mod_file.name)
            if not spec or not spec.loader:
                raise DbtRuntimeError(f"Failed to load python model: {identifier}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Run the model
            dbt_obj = module.dbtObj(load_df_function)
            df = module.model(dbt_obj, local_db)

            # Materialize: convert result to Arrow and ship to GizmoSQL
            if isinstance(df, duckdb.DuckDBPyRelation):
                arrow_table = df.to_arrow_table()
            elif isinstance(df, pa.Table):
                arrow_table = df
            elif hasattr(df, "to_arrow"):
                # pandas DataFrame
                arrow_table = pa.Table.from_pandas(df)
            else:
                raise DbtRuntimeError(
                    f"Python model must return a DuckDB relation, Arrow table, "
                    f"or pandas DataFrame, got {type(df)}"
                )

            # Call materialize from compiled code (handles CREATE TABLE)
            module.materialize(arrow_table, adbc_conn)

        except DbtRuntimeError:
            raise
        except Exception as err:
            raise DbtRuntimeError(
                f"Python model failed:\n{''.join(traceback.format_exception(err))}"
            )
        finally:
            os.unlink(mod_file.name)
            local_db.close()

        return AdapterResponse(_message="OK")

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
