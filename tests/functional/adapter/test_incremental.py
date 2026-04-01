import pytest

from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)


class TestIncrementalUniqueKeyGizmoSQL(BaseIncrementalUniqueKey):
    pass


class TestIncrementalPredicatesGizmoSQL(BaseIncrementalPredicates):
    pass


class TestIncrementalOnSchemaChangeGizmoSQL(BaseIncrementalOnSchemaChange):
    pass
