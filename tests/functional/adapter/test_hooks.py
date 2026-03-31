import pytest

from dbt.tests.adapter.hooks.test_model_hooks import (
    BasePrePostModelHooksOnSeeds,
    BaseHooksRefsOnSeeds,
    BasePrePostModelHooksOnSeedsPlusPrefixed,
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
    BasePrePostModelHooksOnSnapshots,
    BasePrePostSnapshotHooksInConfigKwargs,
    BaseDuplicateHooksInConfigs,
)


class TestPrePostModelHooksOnSeedsGizmoSQL(BasePrePostModelHooksOnSeeds):
    pass


class TestHooksRefsOnSeedsGizmoSQL(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedGizmoSQL(
    BasePrePostModelHooksOnSeedsPlusPrefixed
):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespaceGizmoSQL(
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace
):
    pass


class TestPrePostModelHooksOnSnapshotsGizmoSQL(BasePrePostModelHooksOnSnapshots):
    pass


class TestPrePostSnapshotHooksInConfigKwargsGizmoSQL(
    BasePrePostSnapshotHooksInConfigKwargs
):
    pass


class TestDuplicateHooksInConfigsGizmoSQL(BaseDuplicateHooksInConfigs):
    pass
