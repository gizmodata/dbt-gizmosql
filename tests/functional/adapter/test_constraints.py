import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
)


class GizmoSQLColumnEqualSetup:
    """Override data_types fixture for DuckDB/GizmoSQL type names."""

    @pytest.fixture
    def int_type(self):
        return "INTEGER"

    @pytest.fixture
    def schema_int_type(self):
        return "integer"

    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "boolean", "BOOLEAN"],
            ["'2013-11-03 00:00:00'::timestamp", "timestamp", "TIMESTAMP"],
            ["ARRAY['a','b','c']", "varchar[]", "VARCHAR[]"],
            ["ARRAY[1,2,3]", "integer[]", "INTEGER[]"],
            ["'1'::numeric", "numeric", "DECIMAL"],
        ]


class TestTableConstraintsColumnsEqualGizmoSQL(
    GizmoSQLColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass


class TestViewConstraintsColumnsEqualGizmoSQL(
    GizmoSQLColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    pass


class TestIncrementalConstraintsColumnsEqualGizmoSQL(
    GizmoSQLColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    pass


@pytest.mark.skip(reason="Constraint DDL syntax differs for DuckDB — needs adapter-specific expected_sql")
class TestConstraintsRuntimeDdlEnforcementGizmoSQL(BaseConstraintsRuntimeDdlEnforcement):
    pass


@pytest.mark.skip(reason="Constraint rollback requires transaction support not available via Flight SQL autocommit")
class TestConstraintsRollbackGizmoSQL(BaseConstraintsRollback):
    pass


@pytest.mark.skip(reason="Constraint DDL syntax differs for DuckDB — needs adapter-specific expected_sql")
class TestIncrementalConstraintsRuntimeDdlEnforcementGizmoSQL(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    pass


@pytest.mark.skip(reason="Constraint rollback requires transaction support not available via Flight SQL autocommit")
class TestIncrementalConstraintsRollbackGizmoSQL(BaseIncrementalConstraintsRollback):
    pass


@pytest.mark.skip(reason="Constraint DDL syntax differs for DuckDB — needs adapter-specific expected_sql")
class TestModelConstraintsRuntimeEnforcementGizmoSQL(BaseModelConstraintsRuntimeEnforcement):
    pass
