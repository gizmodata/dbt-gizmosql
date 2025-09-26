import os

import pytest

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'gizmosql',
        'threads': 1,
        'host': os.getenv('GIZMOSQL_HOSTNAME'),
        'username': os.getenv('GIZMOSQL_USERNAME'),
        'password': os.getenv('GIZMOSQL_PASSWORD'),
        'use_encryption': (os.getenv('GIZMOSQL_USE_ENCRYPTION', "true").lower() == 'true'),
        'tls_skip_verify': (os.getenv('GIZMOSQL_TLS_SKIP_VERIFY', "false").lower() == 'true')
    }
