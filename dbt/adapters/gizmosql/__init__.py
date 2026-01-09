from dbt.adapters.base import AdapterPlugin

from dbt.adapters.gizmosql.connections import GizmoSQLConnectionManager  # noqa
from dbt.adapters.gizmosql.connections import GizmoSQLCredentials
from dbt.adapters.gizmosql.impl import GizmoSQLAdapter
from dbt.include import gizmosql

__version__ = "1.11.2"

Plugin = AdapterPlugin(
    adapter=GizmoSQLAdapter,
    credentials=GizmoSQLCredentials,
    include_path=gizmosql.PACKAGE_PATH
)
