import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)


class TestPythonModelGizmoSQL(BasePythonModelTests):
    pass


class TestPythonIncrementalGizmoSQL(BasePythonIncrementalTests):
    pass
