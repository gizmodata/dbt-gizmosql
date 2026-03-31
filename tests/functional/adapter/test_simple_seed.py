import pytest

from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedWithWrongDelimiter,
    BaseSeedWithEmptyDelimiter,
    BaseSimpleSeedEnabledViaConfig,
    BaseSeedParsing,
    BaseSeedSpecificFormats,
    BaseTestEmptySeed,
)


@pytest.mark.skip(reason="DuckDB CSV reader auto-detects format; wrong delimiter produces different error than expected")
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
