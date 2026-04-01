import pytest

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)


class TestPythonModelGizmoSQL(BasePythonModelTests):
    pass


@pytest.mark.skip(reason="Incremental Python model test uses pandas-style df.filter(df.id > 5) which requires pandas/numpy")
class TestPythonIncrementalGizmoSQL(BasePythonIncrementalTests):
    pass
