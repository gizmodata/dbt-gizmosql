"""Functional tests for the `external` materialization.

Covers feature parity with dbt-duckdb's external materialization tests
(default parquet, CSV, JSON, custom location, format inference, delimiter,
empty results, `ref()` from a downstream model) plus dbt-gizmosql-specific
additions (plugin rejection, options dict, partitioning, S3 via MinIO — the
exact examples shown in the README).

All I/O runs server-side on the GizmoSQL container: the adapter writes files
to the container's /tmp (and, in the S3 class, to an s3:// URI backed by a
MinIO sidecar) and we verify the round-trip entirely through GizmoSQL queries.
"""
import os
import time
from pathlib import Path
from uuid import uuid4

import docker
import pytest
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    relation_from_name,
    run_dbt,
)


def _models(results):
    """Filter run_dbt results down to model nodes, excluding on-run-* hooks."""
    out = []
    for r in results:
        node = getattr(r, "node", None)
        rt = getattr(node, "resource_type", None)
        if rt is None or str(rt).endswith("model"):
            out.append(r)
    return out


# A three-row dataset that every external model writes and reads back.
SIMPLE_MODEL_SQL = """
select 1 as id, 'alice' as name, 10.5 as amount
union all
select 2 as id, 'bob' as name, 20.25 as amount
union all
select 3 as id, 'carol' as name, 30.75 as amount
"""

# A regular table-materialized baseline to compare external models against.
TABLE_BASE_SQL = """
{{ config(materialized='table') }}
""" + SIMPLE_MODEL_SQL


class BaseExternal:
    """Shared fixtures: unique external_root per class + on-run-start hook that
    creates it on the server before any external model runs."""

    @pytest.fixture(scope="class")
    def external_root(self):
        # One level below /tmp (which always exists on the container), and
        # unique per test class so we don't collide with prior runs or other
        # classes in the same session.
        return f"/tmp/dbt_gizmosql_ext_{uuid4().hex[:8]}"

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, gizmosql_server, external_root):
        return {
            "type": "gizmosql",
            "threads": 1,
            "host": "localhost",
            "port": 31337,
            "username": "dbt",
            "password": "dbt",
            "database": "dbt",
            "use_encryption": True,
            "tls_skip_verify": True,
            "external_root": external_root,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self, external_root):
        # DuckDB's single-file COPY doesn't create parent directories; the
        # PARTITION_BY form of COPY does (one level deep, parent must exist).
        # We piggyback on that as a portable, server-side "mkdir" so tests
        # don't need volume mounts or docker-exec.
        return {
            "on-run-start": [
                (
                    f"COPY (SELECT 1 AS _bootstrap, 'x' AS _pad) "
                    f"TO '{external_root}' "
                    f"(FORMAT parquet, PARTITION_BY (_bootstrap), OVERWRITE_OR_IGNORE)"
                ),
            ],
        }


# ---------- dbt-duckdb parity: default format, location, delimiter --------- #


class TestExternalBasics(BaseExternal):
    """Mirrors dbt-duckdb's BaseExternalMaterializations: a baseline table
    model plus one external model per supported (format, location) shape, and
    asserts they all return the same rows as the baseline."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            # Baseline — regular table materialization.
            "table_model.sql": TABLE_BASE_SQL,
            # No location, no format → parquet at {external_root}/table_default.parquet
            "table_default.sql": "{{ config(materialized='external') }}\n" + SIMPLE_MODEL_SQL,
            # Explicit format='csv', no location → csv at {external_root}/table_csv.csv
            "table_csv.sql": "{{ config(materialized='external', format='csv') }}\n"
            + SIMPLE_MODEL_SQL,
            # Explicit format='json', no location
            "table_json.sql": "{{ config(materialized='external', format='json') }}\n"
            + SIMPLE_MODEL_SQL,
            # Explicit .parquet location — format inferred from extension
            "table_parquet_location.sql": (
                "{{ config(materialized='external', "
                "location=\"{{ adapter.external_root() }}/test.parquet\") }}\n"
                + SIMPLE_MODEL_SQL
            ),
            # Explicit .csv location + custom delimiter
            "table_csv_location_delim.sql": (
                "{{ config(materialized='external', "
                "location=\"{{ adapter.external_root() }}/test_delim.csv\", "
                "delimiter='|') }}\n"
                + SIMPLE_MODEL_SQL
            ),
            # A downstream model that ref()s an external model — verifies the
            # read-side view is usable as a dbt source by later models.
            "downstream.sql": (
                "{{ config(materialized='table') }}\n"
                "select count(*) as n from {{ ref('table_default') }}"
            ),
        }

    def test_basics(self, project, external_root):
        results = _models(run_dbt(["run"]))
        assert len(results) == 7

        check_relation_types(
            project.adapter,
            {
                "table_model": "table",
                "table_default": "view",
                "table_csv": "view",
                "table_json": "view",
                "table_parquet_location": "view",
                "table_csv_location_delim": "view",
                "downstream": "table",
            },
        )

        # Every external view has the same three rows as the baseline table.
        check_relations_equal(
            project.adapter,
            [
                "table_model",
                "table_default",
                "table_csv",
                "table_json",
                "table_parquet_location",
                "table_csv_location_delim",
            ],
        )

        # Downstream model saw the external view as a ref().
        downstream = relation_from_name(project.adapter, "downstream")
        row = project.run_sql(f"select n from {downstream}", fetch="one")
        assert row[0] == 3

        # The expected data on disk — values, not just counts.
        expected_rows = [(1, "alice", 10.5), (2, "bob", 20.25), (3, "carol", 30.75)]

        # Read each file DIRECTLY (bypassing the dbt view) to prove the bytes
        # on the server's filesystem are what we asked to be written. This
        # catches the hypothetical bug where the view is correct but the file
        # is empty, stale, or wrong — the view could silently paper over that.
        file_readers = {
            f"{external_root}/table_default.parquet": f"read_parquet('{external_root}/table_default.parquet')",
            f"{external_root}/test.parquet": f"read_parquet('{external_root}/test.parquet')",
            f"{external_root}/table_csv.csv": (
                f"read_csv('{external_root}/table_csv.csv', auto_detect=true, header=true)"
            ),
            f"{external_root}/test_delim.csv": (
                f"read_csv('{external_root}/test_delim.csv', delim='|', header=true)"
            ),
            f"{external_root}/table_json.json": (
                f"read_json('{external_root}/table_json.json', auto_detect=true)"
            ),
        }
        for path, reader in file_readers.items():
            # File must actually exist on disk on the server.
            found = project.run_sql(
                f"select count(*) from glob('{path}')", fetch="one"
            )
            assert found[0] == 1, f"expected file at {path}"

            # Read the file directly and verify both row count AND values.
            rows = project.run_sql(
                f"select id, name, amount from {reader} order by id",
                fetch="all",
            )
            # read_csv returns VARCHAR by default → normalize to the same
            # shape as the parquet/json reads before comparing.
            normalized = [(int(r[0]), str(r[1]), float(r[2])) for r in rows]
            assert normalized == expected_rows, (
                f"contents of {path} did not match expected rows: got {normalized}"
            )

        # And as an extra belt-and-braces check, the pipe-delimited CSV is
        # literally pipe-delimited on disk (raw byte inspection via read_text).
        raw = project.run_sql(
            f"select content from read_text('{external_root}/test_delim.csv')",
            fetch="one",
        )
        raw_csv = raw[0]
        assert "|" in raw_csv, f"expected '|' delimiter in raw file, got: {raw_csv!r}"
        assert "id|name|amount" in raw_csv, f"expected header row, got: {raw_csv!r}"
        assert "1|alice|10.5" in raw_csv, f"expected data row, got: {raw_csv!r}"


# ----------------- dbt-duckdb parity: empty-result handling ---------------- #


class TestExternalEmpty(BaseExternal):
    """dbt-duckdb supports external models that happen to return zero rows."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ext_empty.sql": (
                "{{ config(materialized='external') }}\n"
                + SIMPLE_MODEL_SQL
                + " limit 0"
            ),
        }

    def test_empty(self, project, external_root):
        results = _models(run_dbt(["run"]))
        assert len(results) == 1

        check_relation_types(project.adapter, {"ext_empty": "view"})

        ext_empty = relation_from_name(project.adapter, "ext_empty")

        # The view is empty — the padding row the materialization inserts to
        # preserve the schema is filtered out on the read side.
        row = project.run_sql(f"select count(*) from {ext_empty}", fetch="one")
        assert row[0] == 0

        # The file really was written.
        path = f"{external_root}/ext_empty.parquet"
        found = project.run_sql(
            f"select count(*) from glob('{path}')", fetch="one"
        )
        assert found[0] == 1

        # Reading the file DIRECTLY returns exactly the one all-NULL padding
        # row the materialization wrote to preserve the schema — proving we
        # did hit disk, and that the padding strategy is what produced the
        # file's physical row count.
        direct = project.run_sql(
            f"select count(*) from read_parquet('{path}')", fetch="one"
        )
        assert direct[0] == 1
        null_row = project.run_sql(
            f"select id, name, amount from read_parquet('{path}')", fetch="one"
        )
        assert null_row == (None, None, None), f"expected all-null row, got {null_row}"

        # Schema on disk has the right column names and types.
        schema = project.run_sql(
            f"select column_name, column_type from (describe select * from read_parquet('{path}'))",
            fetch="all",
        )
        schema_map = {name: dtype for name, dtype in schema}
        assert set(schema_map) == {"id", "name", "amount"}
        assert "INT" in schema_map["id"].upper()
        assert "VARCHAR" in schema_map["name"].upper() or "STRING" in schema_map["name"].upper()
        assert "DOUBLE" in schema_map["amount"].upper() or "DECIMAL" in schema_map["amount"].upper()

        # And the view correctly filters the padding row out on the read side.
        cols = project.run_sql(
            f"select column_name from (describe select * from {ext_empty})",
            fetch="all",
        )
        col_names = {r[0] for r in cols}
        assert {"id", "name", "amount"}.issubset(col_names)


# --------------- Plugin / glue_register rejection (gizmosql-only) ---------- #


class TestExternalPluginRejected(BaseExternal):
    """dbt-duckdb plugins (glue etc.) are client-side and have no analogue on
    a remote Flight SQL server — setting them should produce a clear error."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ext_with_plugin.sql": (
                "{{ config(materialized='external', plugin='glue') }}\n"
                + SIMPLE_MODEL_SQL
            ),
        }

    def test_plugin_errors(self, project):
        results = _models(run_dbt(["run"], expect_pass=False))
        assert len(results) == 1
        assert results[0].status != "success"
        msg = (results[0].message or "").lower()
        assert "plugin" in msg and "not supported" in msg


class TestExternalGlueRegisterRejected(BaseExternal):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ext_with_glue.sql": (
                "{{ config(materialized='external', glue_register=true) }}\n"
                + SIMPLE_MODEL_SQL
            ),
        }

    def test_glue_errors(self, project):
        results = _models(run_dbt(["run"], expect_pass=False))
        assert len(results) == 1
        assert results[0].status != "success"
        msg = (results[0].message or "").lower()
        assert "glue" in msg and "not supported" in msg


# ---------------- README example: options dict (compression) --------------- #


class TestReadmeParquetZstd(BaseExternal):
    """Exercises the README example that sets a compression codec via
    `options={'compression': 'zstd'}`.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "readme_zstd.sql": (
                "{{ config(materialized='external', format='parquet', "
                "options={'compression': 'zstd'}) }}\n"
                + SIMPLE_MODEL_SQL
            ),
        }

    def test_zstd(self, project, external_root):
        results = _models(run_dbt(["run"]))
        assert len(results) == 1

        path = f"{external_root}/readme_zstd.parquet"
        found = project.run_sql(
            f"select count(*) from glob('{path}')", fetch="one"
        )
        assert found[0] == 1

        # Every column in the parquet file uses zstd compression.
        compressions = project.run_sql(
            f"select distinct compression from parquet_metadata('{path}')",
            fetch="all",
        )
        codecs = {row[0].lower() for row in compressions}
        assert codecs == {"zstd"}, f"expected only zstd, got {codecs}"

        # Direct file-read asserts the bytes themselves, not just the
        # read-side view — row count AND values.
        direct = project.run_sql(
            f"select id, name, amount from read_parquet('{path}') order by id",
            fetch="all",
        )
        assert direct == [(1, "alice", 10.5), (2, "bob", 20.25), (3, "carol", 30.75)]

        # And the view reads back the same rows.
        readme_zstd = relation_from_name(project.adapter, "readme_zstd")
        rows = project.run_sql(
            f"select count(*) from {readme_zstd}", fetch="one"
        )
        assert rows[0] == 3


# -------------- README example: partitioning via options dict -------------- #


PARTITIONED_MODEL_SQL = """
{{ config(
    materialized='external',
    format='parquet',
    options={'partition_by': 'year, month', 'compression': 'zstd'}
) }}
select 2024 as year, 1 as month, 'A' as name, 10 as value
union all
select 2024 as year, 2 as month, 'B' as name, 20 as value
union all
select 2025 as year, 1 as month, 'C' as name, 30 as value
union all
select 2025 as year, 1 as month, 'D' as name, 40 as value
"""


class TestReadmePartitioned(BaseExternal):
    """Exercises the README partitioning example end-to-end."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"readme_partitioned.sql": PARTITIONED_MODEL_SQL}

    def test_partitioned(self, project, external_root):
        results = _models(run_dbt(["run"]))
        assert len(results) == 1

        check_relation_types(
            project.adapter, {"readme_partitioned": "view"}
        )

        partitioned = relation_from_name(project.adapter, "readme_partitioned")

        # The view reads across all partitions and returns 4 rows.
        row = project.run_sql(
            f"select count(*) from {partitioned}", fetch="one"
        )
        assert row[0] == 4

        # Partitions are laid out as Hive-style directories on disk.
        root = f"{external_root}/readme_partitioned"
        year_dirs = project.run_sql(
            f"select distinct regexp_extract(file, 'year=(\\d+)', 1) "
            f"from glob('{root}/**/*.parquet') order by 1",
            fetch="all",
        )
        years = {row[0] for row in year_dirs}
        assert years == {"2024", "2025"}

        month_dirs = project.run_sql(
            f"select distinct regexp_extract(file, 'month=(\\d+)', 1) "
            f"from glob('{root}/**/*.parquet') order by 1",
            fetch="all",
        )
        months = {row[0] for row in month_dirs}
        assert months == {"1", "2"}

        # Each partition's rows survived the round-trip (through the view).
        by_partition = project.run_sql(
            f"select year, month, count(*) as n from {partitioned} "
            "group by 1, 2 order by 1, 2",
            fetch="all",
        )
        assert by_partition == [
            (2024, 1, 1),
            (2024, 2, 1),
            (2025, 1, 2),
        ]

        # Direct file read — skip the view, hive_partitioning=true reattaches
        # the partition columns from the directory names so we can verify the
        # complete row contents as they live on disk.
        direct = project.run_sql(
            f"select year, month, name, value from "
            f"read_parquet('{root}/**/*.parquet', hive_partitioning=true) "
            "order by year, month, name",
            fetch="all",
        )
        assert direct == [
            (2024, 1, "A", 10),
            (2024, 2, "B", 20),
            (2025, 1, "C", 30),
            (2025, 1, "D", 40),
        ]

        # And every partition parquet file uses the requested zstd codec.
        codecs = project.run_sql(
            f"select distinct compression from "
            f"parquet_metadata('{root}/**/*.parquet')",
            fetch="all",
        )
        codec_set = {row[0].lower() for row in codecs}
        assert codec_set == {"zstd"}, f"expected zstd on all partitions, got {codec_set}"


# -------------------- S3 (via MinIO) — end-to-end proof -------------------- #


MINIO_PORT = 9000
MINIO_USER = "minioadmin"
MINIO_PASSWORD = "minioadmin"
S3_BUCKET = "dbt-gizmosql-test"
MINIO_NETWORK_NAME = "dbt-gizmosql-test-net"
MINIO_ALIAS = "minio-test"  # Reachable by name from the GizmoSQL container.


@pytest.fixture(scope="class")
def minio_server(gizmosql_server, tmp_path_factory):
    """Spin up a MinIO container on a shared docker network with GizmoSQL.

    Using a user-defined bridge network (rather than publishing MinIO's port
    to the host and using `host.docker.internal`) means the test works the
    same on Linux CI and macOS Docker Desktop without relying on
    host-gateway magic or risking port collisions on the CI runner.

    The bucket is pre-created via a tmpdir bind-mount, which doubles as the
    host-side spy we use to verify that the server actually wrote objects.
    """
    client = docker.from_env()

    # Create (or reuse) a user-defined bridge network that both containers
    # sit on. Container name resolution works on user-defined bridges.
    try:
        network = client.networks.get(MINIO_NETWORK_NAME)
    except docker.errors.NotFound:
        network = client.networks.create(MINIO_NETWORK_NAME, driver="bridge")

    # Attach the existing GizmoSQL container to the shared network so it can
    # resolve the MinIO alias — no-op if already attached.
    try:
        network.connect(gizmosql_server, aliases=["gizmosql-test"])
    except docker.errors.APIError as err:
        if "already exists" not in str(err).lower():
            raise

    # Drop any stale MinIO container from a prior aborted run.
    try:
        existing = client.containers.get("dbt-gizmosql-test-minio")
        existing.remove(force=True)
    except docker.errors.NotFound:
        pass

    storage_root = tmp_path_factory.mktemp("minio-data")
    bucket_dir = storage_root / S3_BUCKET
    bucket_dir.mkdir()

    container = client.containers.run(
        image="minio/minio:latest",
        name="dbt-gizmosql-test-minio",
        command="server /data",
        detach=True,
        remove=True,
        tty=True,
        network=MINIO_NETWORK_NAME,
        environment={
            "MINIO_ROOT_USER": MINIO_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
        },
        volumes={str(storage_root): {"bind": "/data", "mode": "rw"}},
    )

    # Give the container an explicit network alias the GizmoSQL server can
    # resolve — belt-and-braces over the auto-assigned container-name alias.
    try:
        network.disconnect(container)
        network.connect(container, aliases=[MINIO_ALIAS])
    except docker.errors.APIError:
        pass

    try:
        deadline = time.time() + 30
        ready = False
        while time.time() < deadline:
            logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace")
            if "API:" in logs:
                ready = True
                break
            time.sleep(0.5)
        if not ready:
            logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace")
            raise TimeoutError(f"MinIO did not become ready in 30s. Last logs:\n{logs}")

        yield {"container": container, "storage_root": storage_root, "bucket_dir": bucket_dir}
    finally:
        try:
            container.stop()
        except Exception:
            pass
        try:
            network.disconnect(gizmosql_server, force=True)
        except Exception:
            pass
        try:
            network.remove()
        except Exception:
            pass


class TestExternalS3(BaseExternal):
    """End-to-end proof that `external` materializations can write to an
    S3-compatible object store (MinIO here). The GizmoSQL server installs
    httpfs, creates a secret pointing at MinIO, and COPYs a parquet file to
    s3://dbt-gizmosql-test/... entirely server-side. We then verify:

      1. The object really landed in MinIO (host-side filesystem inspection).
      2. GizmoSQL can read it back via `read_parquet('s3://...')`.
      3. The dbt view over the S3 object returns the expected rows.
    """

    @pytest.fixture(scope="class")
    def s3_prefix(self):
        return f"run-{uuid4().hex[:8]}"

    @pytest.fixture(scope="class")
    def external_root(self, minio_server, s3_prefix):
        return f"s3://{S3_BUCKET}/{s3_prefix}"

    @pytest.fixture(scope="class")
    def project_config_update(self, external_root):
        # Two server-side setup steps run before any model materializes:
        #   1. Install + load httpfs (DuckDB's S3 client extension).
        #   2. Create a secret pointing the S3 client at the MinIO sidecar,
        #      addressed by its docker network alias.
        return {
            "on-run-start": [
                "INSTALL httpfs",
                "LOAD httpfs",
                (
                    "CREATE OR REPLACE SECRET minio_test ("
                    "  TYPE S3,"
                    f"  KEY_ID '{MINIO_USER}',"
                    f"  SECRET '{MINIO_PASSWORD}',"
                    f"  ENDPOINT '{MINIO_ALIAS}:{MINIO_PORT}',"
                    "  URL_STYLE 'path',"
                    "  USE_SSL false"
                    ")"
                ),
            ],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "s3_model.sql": (
                "{{ config(materialized='external', format='parquet') }}\n"
                + SIMPLE_MODEL_SQL
            ),
        }

    def test_s3_write_and_read(self, project, minio_server, s3_prefix):
        results = _models(run_dbt(["run"]))
        assert len(results) == 1
        assert results[0].status == "success", results[0].message

        check_relation_types(project.adapter, {"s3_model": "view"})

        # 1. The object really landed on the object store — verify by peeking
        #    at the host directory backing the MinIO bucket.
        bucket_dir: Path = minio_server["bucket_dir"]
        written = sorted(p.relative_to(bucket_dir) for p in bucket_dir.rglob("*.parquet"))
        assert written, f"no parquet files found under {bucket_dir}"
        assert any(s3_prefix in str(p) for p in written), (
            f"expected a file under prefix {s3_prefix}, found {written}"
        )
        assert any(p.name == "s3_model.parquet" for p in written), (
            f"expected s3_model.parquet, found {written}"
        )

        s3_url = f"s3://{S3_BUCKET}/{s3_prefix}/s3_model.parquet"

        # 2. The GizmoSQL server reads the same S3 URL back and gets the
        #    exact rows we wrote (bypassing the dbt view so we're sure the
        #    bytes on object storage are correct).
        direct = project.run_sql(
            f"select id, name, amount from read_parquet('{s3_url}') order by id",
            fetch="all",
        )
        assert direct == [(1, "alice", 10.5), (2, "bob", 20.25), (3, "carol", 30.75)]

        # 3. And the dbt-created view returns the same rows via ref()-ability.
        view = relation_from_name(project.adapter, "s3_model")
        rows = project.run_sql(
            f"select id, name, amount from {view} order by id", fetch="all"
        )
        assert rows == [(1, "alice", 10.5), (2, "bob", 20.25), (3, "carol", 30.75)]
