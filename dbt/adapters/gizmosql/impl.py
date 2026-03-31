import time
from typing import List

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.column import Column
from dbt.adapters.base.meta import available
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLAdapter as adapter_cls

from dbt.adapters.gizmosql import GizmoSQLConnectionManager
from dbt.adapters.gizmosql.column import DuckDBColumn
from dbt.adapters.gizmosql.relation import GizmoSQLRelation

logger = AdapterLogger("GizmoSQL")

_CATALOG_RETRY_LIMIT = 5
_CATALOG_RETRY_BACKOFF_SECONDS = 1.0


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

    def get_columns_in_relation(self, relation: BaseRelation) -> List[Column]:
        """Retry when information_schema returns no columns for a relation.

        Over Flight SQL, the catalog (information_schema) may not immediately
        reflect a recently auto-committed CREATE TABLE. A real table always has
        at least one column, so an empty result indicates a propagation delay.
        """
        for attempt in range(_CATALOG_RETRY_LIMIT):
            columns = super().get_columns_in_relation(relation)
            if columns:
                return columns
            delay = _CATALOG_RETRY_BACKOFF_SECONDS * (attempt + 1)
            logger.debug(
                f"get_columns_in_relation returned empty for {relation} "
                f"(attempt {attempt + 1}/{_CATALOG_RETRY_LIMIT}), "
                f"retrying in {delay:.1f}s"
            )
            time.sleep(delay)
        return []

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
