"""
Regression test for concurrent connection opening.

The Go ADBC Flight SQL driver has an upstream bug (apache/arrow-adbc#1178) where
concurrent adbc_get_info() calls crash with "fatal error: concurrent map writes"
due to an unprotected map in DriverInfo. GizmoSQLConnectionManager.open() works
around this by calling adbc_get_info() exactly once behind a lock.
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.gizmosql.connections import GizmoSQLConnectionManager, GizmoSQLCredentials
from tests.conftest import GIZMOSQL_PORT


@pytest.fixture()
def credentials(gizmosql_server):
    """Build GizmoSQLCredentials pointing at the test container."""
    return GizmoSQLCredentials(
        host="localhost",
        port=GIZMOSQL_PORT,
        username="dbt",
        password="dbt",
        use_encryption=True,
        tls_skip_verify=True,
    )


@pytest.fixture(autouse=True)
def reset_vendor_check():
    """Reset the cached vendor check so each test exercises the lock path."""
    GizmoSQLConnectionManager._vendor_version_verified = False
    yield
    GizmoSQLConnectionManager._vendor_version_verified = False


def _open_connection(credentials, name="test"):
    conn = Connection(type="gizmosql", name=name, credentials=credentials)
    GizmoSQLConnectionManager.open(conn)
    return conn


class TestConcurrentConnections:
    def test_concurrent_open(self, credentials):
        """
        Opening connections from multiple threads must not crash.

        Without the cached vendor check, concurrent adbc_get_info() calls
        trigger a Go runtime panic ("concurrent map writes") in the ADBC driver.
        """
        num_threads = 8
        num_iterations = 5
        errors = []

        for iteration in range(num_iterations):
            GizmoSQLConnectionManager._vendor_version_verified = False
            iteration_barrier = threading.Barrier(num_threads)

            def open_and_query(thread_id, barrier=iteration_barrier):
                barrier.wait()
                conn = _open_connection(credentials, name=f"thread-{thread_id}")
                try:
                    cursor = conn.handle.cursor()
                    cursor.execute(f"SELECT {thread_id} AS tid")
                    result = cursor.fetchall()
                    cursor.close()
                    return result
                finally:
                    conn.handle.close()

            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {
                    executor.submit(open_and_query, i): i
                    for i in range(num_threads)
                }
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        errors.append(
                            f"iteration={iteration} thread={futures[future]}: {e}"
                        )

        assert not errors, f"Got {len(errors)} errors:\n" + "\n".join(errors[:5])
