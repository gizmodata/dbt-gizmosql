import pytest

from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
)


class TestEmptyGizmoSQL(BaseTestEmpty):
    pass


@pytest.mark.skip(reason="dbt-core generates invalid SQL with double alias for DuckDB syntax")
class TestEmptyInlineSourceRefGizmoSQL(BaseTestEmptyInlineSourceRef):
    pass
