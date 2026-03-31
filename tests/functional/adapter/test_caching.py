import pytest

from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingSelectedSchemaOnly,
    BaseNoPopulateCache,
)


class TestCachingLowercaseModelGizmoSQL(BaseCachingLowercaseModel):
    pass


class TestCachingSelectedSchemaOnlyGizmoSQL(BaseCachingSelectedSchemaOnly):
    pass


class TestNoPopulateCacheGizmoSQL(BaseNoPopulateCache):
    pass
