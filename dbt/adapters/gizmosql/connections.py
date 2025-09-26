from contextlib import contextmanager
from dataclasses import dataclass, field

import dbt.adapters.exceptions
import dbt.exceptions  # noqa
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions
from dbt.adapters.base.connections import AdapterResponse
from dbt.adapters.contracts.connection import Connection, ConnectionState, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLConnectionManager
from pydantic import Field
from typing import Optional


logger = AdapterLogger("GizmoSQL")


@dataclass
class GizmoSQLCredentials(Credentials):
    database: str = ""          # default so it's not required
    schema: str = "main"

    host: str = field(kw_only=True)
    username: str = field(kw_only=True)
    password: str = field(kw_only=True)
    port: int = field(default=31337, kw_only=True)
    use_encryption: bool = field(default=True, kw_only=True)
    tls_skip_verify: bool = field(default=False, kw_only=True)
    catalog: Optional[str] = field(default=None, kw_only=True)

    _ALIASES = {
        "pass": "password",
        "user": "username",
        "use_tls": "use_encryption",
        "disable_certificate_verification": "tls_skip_verify",
    }

    @property
    def type(self):
        """Return name of adapter."""
        return "gizmosql"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.host

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("host", "port", "schema", "database", "user", "catalog", "use_encryption", "tls_skip_verify")


class GizmoSQLConnectionManager(SQLConnectionManager):
    TYPE = "gizmosql"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials: GizmoSQLCredentials = connection.credentials
        tls_string = ""
        if credentials.use_encryption:
            tls_string = "+tls"

        connect_kwargs = dict(uri=f"grpc{tls_string}://{credentials.host}:{credentials.port}",
                              db_kwargs={"username": credentials.username,
                                         "password": credentials.password,
                                         DatabaseOptions.TLS_SKIP_VERIFY.value: str(
                                             credentials.tls_skip_verify).lower(),
                                         },
                              autocommit=False
                              )
        if credentials.catalog:
            connect_kwargs.update(conn_kwargs={"adbc.connection.catalog": credentials.catalog})

        try:
            handle = gizmosql.connect(
                **connect_kwargs
            )
            credentials.catalog = getattr(handle, "adbc_current_catalog")
            credentials.database = credentials.catalog

            connection.handle = handle
            connection.state = ConnectionState.OPEN

            return connection

        except RuntimeError as e:
            logger.debug(f"Got an error when attempting to connect to GizmoSQL: '{e}'")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise dbt.adapters.exceptions.FailedToConnectError(str(e))

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        # if the connection is in closed or init, there's nothing to do
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        connection = super(SQLConnectionManager, cls).close(connection)
        return connection

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        message = "OK"
        return AdapterResponse(_message=message)

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        connection.handle.adbc_cancel()
        logger.debug(f"query cancelled on connection {connection.name}")

    @contextmanager
    def exception_handler(self, sql: str, connection_name="master"):
        try:
            yield
        except dbt.exceptions.DbtRuntimeError:
            raise
        except RuntimeError as e:
            logger.debug("GizmoSQL error: {}".format(str(e)))
            logger.debug("Error running SQL: {}".format(sql))
            # Preserve original RuntimeError with full context instead of swallowing
            raise dbt.exceptions.DbtRuntimeError(str(e)) from e
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            raise dbt.exceptions.DbtRuntimeError(str(exc)) from exc
