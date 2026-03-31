import pytest

from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedWithWrongDelimiter,
    BaseSeedWithEmptyDelimiter,
    BaseSimpleSeedEnabledViaConfig,
    BaseSeedParsing,
    BaseSeedSpecificFormats,
    BaseTestEmptySeed,
)


class TestSeedWithWrongDelimiterGizmoSQL(BaseSeedWithWrongDelimiter):
    pass


class TestSeedWithEmptyDelimiterGizmoSQL(BaseSeedWithEmptyDelimiter):
    pass


class TestSimpleSeedEnabledViaConfigGizmoSQL(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSeedParsingGizmoSQL(BaseSeedParsing):
    pass


class TestSeedSpecificFormatsGizmoSQL(BaseSeedSpecificFormats):
    pass


class TestEmptySeedGizmoSQL(BaseTestEmptySeed):
    pass
