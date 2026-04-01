import pytest

from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
)


class TestEmptyGizmoSQL(BaseTestEmpty):
    pass


@pytest.mark.skip(reason="dbt-core generates double-alias SQL incompatible with DuckDB parser")
class TestEmptyInlineSourceRefGizmoSQL(BaseTestEmptyInlineSourceRef):
    pass
