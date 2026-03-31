import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Tuple, Any, Optional

if TYPE_CHECKING:
    import agate

import dbt.adapters.exceptions
import dbt.exceptions  # noqa
from adbc_driver_gizmosql import dbapi as gizmosql
from dbt.adapters.base.connections import AdapterResponse
from dbt.adapters.contracts.connection import Connection, ConnectionState, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLConnectionManager

logger = AdapterLogger("GizmoSQL")


@dataclass
class GizmoSQLCredentials(Credentials):
    database: str = ""
    schema: str = ""

    host: str = field(kw_only=True)
    username: Optional[str] = field(default=None, kw_only=True)
    password: Optional[str] = field(default=None, kw_only=True)
    port: int = field(default=31337, kw_only=True)
    use_encryption: bool = field(default=True, kw_only=True)
    tls_skip_verify: bool = field(default=False, kw_only=True)
    auth_type: Optional[str] = field(default=None, kw_only=True)

    _ALIASES = {
        "catalog": "database",
        "dbname": "database",
        "pass": "password",
        "user": "username",
        "use_tls": "use_encryption",
        "disable_certificate_verification": "tls_skip_verify",
    }

    def __post_init__(self):
        # Set the default database and schema if they are not set by retrieving them from the server
        if not self.database or not self.schema:
            tls_string = ""
            if self.use_encryption:
                tls_string = "+tls"

            connect_kwargs = {
                "uri": f"grpc{tls_string}://{self.host}:{self.port}",
                "tls_skip_verify": self.tls_skip_verify,
                "conn_kwargs": {"adbc.connection.catalog": self.database} if self.database else None,
                "autocommit": False,
            }
            if self.auth_type:
                connect_kwargs["auth_type"] = self.auth_type
            if self.username:
                connect_kwargs["username"] = self.username
                connect_kwargs["password"] = self.password or ""

            with gizmosql.connect(**connect_kwargs) as conn:
                self.database = self.database or getattr(conn, "adbc_current_catalog")
                self.schema = self.schema or getattr(conn, "adbc_current_db_schema")

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
        return ("host", "port", "schema", "database", "user", "use_encryption", "tls_skip_verify", "auth_type")


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

        try:
            connect_kwargs = {
                "uri": f"grpc{tls_string}://{credentials.host}:{credentials.port}",
                "tls_skip_verify": credentials.tls_skip_verify,
                "conn_kwargs": {"adbc.connection.catalog": credentials.database} if credentials.database else None,
                "autocommit": True,
            }
            if credentials.auth_type:
                connect_kwargs["auth_type"] = credentials.auth_type
            if credentials.username:
                connect_kwargs["username"] = credentials.username
                connect_kwargs["password"] = credentials.password or ""

            connection.handle = gizmosql.connect(**connect_kwargs)
            connection.state = ConnectionState.OPEN

            vendor_version = connection.handle.adbc_get_info().get("vendor_version")

            if not re.search(pattern="^duckdb ", string=vendor_version):
                raise RuntimeError(f"Unsupported GizmoSQL server backend: '{vendor_version}'")

            return connection

        except RuntimeError as e:
            logger.debug(f"Got an error when attempting to connect to GizmoSQL: '{e}'")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise dbt.adapters.exceptions.FailedToConnectError(str(e))

    def add_begin_query(self):
        """No-op: rely on autocommit so each statement commits immediately.

        Flight SQL's PREPARE phase validates against committed catalog state,
        so uncommitted DDL from earlier in the same transaction is invisible
        to subsequent statements. By skipping BEGIN (with autocommit=True on
        the connection), each statement auto-commits and is immediately visible.
        """
        pass

    def add_commit_query(self):
        """No-op: paired with add_begin_query — see docstring there."""
        pass

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        from dbt_common.clients.agate_helper import empty_table

        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        try:
            response = self.get_response(cursor)
            if fetch:
                table = self.get_result_from_cursor(cursor, limit)
            else:
                table = empty_table()
            return response, table
        finally:
            cursor.close()

    def add_select_query(self, sql: str) -> Tuple[Connection, Any]:
        sql = self._add_query_comment(sql)
        return self.add_query(sql, auto_begin=False)

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        # if the connection is in closed or init, there's nothing to do
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        try:
            connection.handle.adbc_cancel()
        except Exception:
            pass
        connection = super().close(connection)
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
