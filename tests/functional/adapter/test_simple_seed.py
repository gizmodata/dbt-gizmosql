import pytest

from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedWithWrongDelimiter,
    BaseSeedWithEmptyDelimiter,
    BaseSimpleSeedEnabledViaConfig,
    BaseSeedParsing,
    BaseSeedSpecificFormats,
    BaseTestEmptySeed,
)


@pytest.mark.skip(reason="DuckDB auto_detect overrides explicit delimiter; test expects syntax error")
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
