
from dbt.adapters.sql import SQLAdapter as adapter_cls

from dbt.adapters.gizmosql import GizmoSQLConnectionManager
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
