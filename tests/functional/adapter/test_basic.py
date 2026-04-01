import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
)
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    no_stats,
    expected_references_catalog,
)


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


class TestIncrementalNotSchemaChangeGizmoSQL(BaseIncrementalNotSchemaChange):
    pass


class TestGenericTestsGizmoSQL(BaseGenericTests):
    pass


class TestSnapshotCheckColsGizmoSQL(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampGizmoSQL(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodGizmoSQL(BaseAdapterMethod):
    pass


class TestValidateConnectionGizmoSQL(BaseValidateConnection):
    pass


class TestDocsGenerateGizmoSQL(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role=None,
            id_type="BIGINT",
            text_type="VARCHAR",
            time_type="TIMESTAMP",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )


class TestDocsGenReferencesGizmoSQL(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_references_catalog(
            project,
            role=None,
            id_type="BIGINT",
            text_type="VARCHAR",
            time_type="TIMESTAMP",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
            bigint_type="BIGINT",
        )
