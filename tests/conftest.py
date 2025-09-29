import os
import pytest
import docker
import time

pytest_plugins = ["dbt.tests.fixtures.project"]

# Constants
GIZMOSQL_PORT = 31337


# Function to wait for a specific log message indicating the container is ready
def wait_for_container_log(container, timeout=30, poll_interval=1, ready_message="GizmoSQL server - started"):
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Get the logs from the container
        logs = container.logs().decode('utf-8')

        # Check if the ready message is in the logs
        if ready_message in logs:
            return True

        # Wait for the next poll
        time.sleep(poll_interval)

    raise TimeoutError(f"Container did not show '{ready_message}' in logs within {timeout} seconds.")


@pytest.fixture(scope="session")
def gizmosql_server():
    client = docker.from_env()
    container = client.containers.run(
        image="gizmodata/gizmosql:latest",
        name="dbt-gizmosql-test",
        detach=True,
        remove=True,
        tty=True,
        init=True,
        ports={f"{GIZMOSQL_PORT}/tcp": GIZMOSQL_PORT},
        environment={"GIZMOSQL_USERNAME": "dbt",
                     "GIZMOSQL_PASSWORD": "dbt",
                     "TLS_ENABLED": "1",
                     "PRINT_QUERIES": "1"
                     },
        stdout=True,
        stderr=True
    )

    # Wait for the container to be ready
    wait_for_container_log(container)

    yield container

    container.stop()


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target(gizmosql_server):
    return {
        'type': 'gizmosql',
        'threads': 1,
        'host': "localhost",
        'port': GIZMOSQL_PORT,
        'username': "dbt",
        'password': "dbt",
        'use_encryption': True,
        'tls_skip_verify': True
    }
