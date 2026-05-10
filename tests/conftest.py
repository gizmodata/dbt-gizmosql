import pytest
import gizmosql

pytest_plugins = ["dbt.tests.fixtures.project"]

GIZMOSQL_DATABASE = "dbt.db"


@pytest.fixture(scope="session")
def gizmosql_server():
    with gizmosql.Server(
        username="dbt",
        password="dbt",
        database_filename=GIZMOSQL_DATABASE,
        extra_env={"PRINT_QUERIES": "1"},
    ) as srv:
        yield srv


@pytest.fixture(scope="class")
def dbt_profile_target(gizmosql_server):
    return {
        'type': 'gizmosql',
        'threads': 1,
        'host': gizmosql_server.host,
        'port': gizmosql_server.port,
        'username': gizmosql_server.username,
        'password': gizmosql_server.password,
        'database': "dbt",
        'use_encryption': False,
    }
