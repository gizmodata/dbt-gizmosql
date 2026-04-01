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
        return "INT"

    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "bool", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamp", "TIMESTAMP", "TIMESTAMP"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"],
            ["ARRAY['a','b','c']", "VARCHAR[]", "VARCHAR[]"],
            ["ARRAY[1,2,3]", "INTEGER[]", "INTEGER[]"],
            ["'1'::numeric", "numeric", "DECIMAL"],
            [
                """'{"bar": "baz", "balance": 7.77, "active": false}'::json""",
                "json",
                "JSON",
            ],
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


@pytest.mark.skip(reason="Flight SQL PREPARE cannot handle CREATE TABLE with FOREIGN KEY references")
class TestConstraintsRuntimeDdlEnforcementGizmoSQL(
    GizmoSQLColumnEqualSetup, BaseConstraintsRuntimeDdlEnforcement
):
    pass


class TestConstraintsRollbackGizmoSQL(GizmoSQLColumnEqualSetup, BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip(reason="Flight SQL PREPARE cannot handle CREATE TABLE with FOREIGN KEY references")
class TestIncrementalConstraintsRuntimeDdlEnforcementGizmoSQL(
    GizmoSQLColumnEqualSetup, BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    pass


class TestIncrementalConstraintsRollbackGizmoSQL(
    GizmoSQLColumnEqualSetup, BaseIncrementalConstraintsRollback
):
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NOT NULL constraint failed"]


@pytest.mark.skip(reason="Flight SQL PREPARE cannot handle CREATE TABLE with FOREIGN KEY references")
class TestModelConstraintsRuntimeEnforcementGizmoSQL(
    GizmoSQLColumnEqualSetup, BaseModelConstraintsRuntimeEnforcement
):
    pass
