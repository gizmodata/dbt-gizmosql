import pytest

from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliases,
    BaseAliasErrors,
    BaseSameAliasDifferentSchemas,
    BaseSameAliasDifferentDatabases,
)


class TestAliasesGizmoSQL(BaseAliases):
    pass


class TestAliasErrorsGizmoSQL(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemasGizmoSQL(BaseSameAliasDifferentSchemas):
    pass


class TestSameAliasDifferentDatabasesGizmoSQL(BaseSameAliasDifferentDatabases):
    pass
