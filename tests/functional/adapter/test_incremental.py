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


@pytest.mark.skip(reason="Incremental predicates use delete+insert which differs from default merge strategy")
class TestIncrementalPredicatesGizmoSQL(BaseIncrementalPredicates):
    pass


@pytest.mark.skip(reason="Schema change handling (append/sync columns) not yet implemented for GizmoSQL")
class TestIncrementalOnSchemaChangeGizmoSQL(BaseIncrementalOnSchemaChange):
    pass
