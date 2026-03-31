import pytest

from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSnapshotCheck,
)
from dbt.tests.adapter.simple_snapshot.new_record_check_mode import (
    BaseSnapshotNewRecordCheckMode,
)
from dbt.tests.adapter.simple_snapshot.new_record_dbt_valid_to_current import (
    BaseSnapshotNewRecordDbtValidToCurrent,
)
from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    BaseSnapshotNewRecordTimestampMode,
)
from dbt.tests.adapter.simple_snapshot.test_various_configs import (
    BaseSnapshotColumnNames,
    BaseSnapshotColumnNamesFromDbtProject,
)


class TestSimpleSnapshotGizmoSQL(BaseSimpleSnapshot):
    pass


class TestSnapshotCheckGizmoSQL(BaseSnapshotCheck):
    pass


class TestSnapshotNewRecordCheckModeGizmoSQL(BaseSnapshotNewRecordCheckMode):
    pass


class TestSnapshotNewRecordDbtValidToCurrentGizmoSQL(
    BaseSnapshotNewRecordDbtValidToCurrent
):
    pass


class TestSnapshotNewRecordTimestampModeGizmoSQL(BaseSnapshotNewRecordTimestampMode):
    pass


class TestSnapshotColumnNamesGizmoSQL(BaseSnapshotColumnNames):
    pass


class TestSnapshotColumnNamesFromDbtProjectGizmoSQL(
    BaseSnapshotColumnNamesFromDbtProject
):
    pass


