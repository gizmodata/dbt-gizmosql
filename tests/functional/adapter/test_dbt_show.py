import pytest

from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowLimit,
    BaseShowSqlHeader,
)


class TestShowLimitGizmoSQL(BaseShowLimit):
    pass


class TestShowSqlHeaderGizmoSQL(BaseShowSqlHeader):
    pass
