import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


class TestSimpleMaterializationsGizmoSQL(BaseSimpleMaterializations):
    pass


class TestSingularTestsGizmoSQL(BaseSingularTests):
    pass


class TestSingularTestsEphemeralGizmoSQL(BaseSingularTestsEphemeral):
    pass


class TestEmptyGizmoSQL(BaseEmpty):
    pass


class TestEphemeralGizmoSQL(BaseEphemeral):
    pass


class TestIncrementalGizmoSQL(BaseIncremental):
    pass


class TestGenericTestsGizmoSQL(BaseGenericTests):
    pass


class TestSnapshotCheckColsGizmoSQL(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampGizmoSQL(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodGizmoSQL(BaseAdapterMethod):
    pass
