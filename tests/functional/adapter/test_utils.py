import pytest

from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_cast import BaseCast
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import (
    BaseCurrentTimestampAware,
)
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_generate_series import BaseGenerateSeries
from dbt.tests.adapter.utils.test_get_powers_of_two import BaseGetPowersOfTwo
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_null_compare import (
    BaseMixedNullCompare,
    BaseNullCompare,
)
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt


class TestAnyValueGizmoSQL(BaseAnyValue):
    pass


class TestCastGizmoSQL(BaseCast):
    pass


class TestCastBoolToTextGizmoSQL(BaseCastBoolToText):
    pass


class TestConcatGizmoSQL(BaseConcat):
    pass


class TestCurrentTimestampAwareGizmoSQL(BaseCurrentTimestampAware):
    pass


class TestEscapeSingleQuotesQuoteGizmoSQL(BaseEscapeSingleQuotesQuote):
    pass


class TestExceptGizmoSQL(BaseExcept):
    pass


class TestGenerateSeriesGizmoSQL(BaseGenerateSeries):
    pass


class TestGetPowersOfTwoGizmoSQL(BaseGetPowersOfTwo):
    pass


class TestHashGizmoSQL(BaseHash):
    pass


class TestIntersectGizmoSQL(BaseIntersect):
    pass


class TestLengthGizmoSQL(BaseLength):
    pass


class TestMixedNullCompareGizmoSQL(BaseMixedNullCompare):
    pass


class TestNullCompareGizmoSQL(BaseNullCompare):
    pass


class TestPositionGizmoSQL(BasePosition):
    pass


class TestSafeCastGizmoSQL(BaseSafeCast):
    pass


class TestStringLiteralGizmoSQL(BaseStringLiteral):
    pass


class TestTypeBigIntGizmoSQL(BaseTypeBigInt):
    pass


class TestTypeBooleanGizmoSQL(BaseTypeBoolean):
    pass


class TestTypeIntGizmoSQL(BaseTypeInt):
    pass
