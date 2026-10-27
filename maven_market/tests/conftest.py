"""This file configures pytest, initializes Databricks Connect if available,
and provides fixtures for Spark and loading test data.
"""

import csv
import json
import os
import pathlib
import sys
from contextlib import contextmanager

try:
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    HAS_DB_CONNECT = True
except ImportError:
    HAS_DB_CONNECT = False
    SparkSession = None

import pytest


@pytest.fixture()
def spark():
    """Provide a SparkSession fixture for tests if Databricks Connect is available."""
    if not HAS_DB_CONNECT:
        pytest.skip("databricks-connect is not installed; skipping live Spark test.")
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture()
def load_fixture(spark):
    """Provide a callable to load JSON or CSV from fixtures/ directory."""
    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    if not HAS_DB_CONNECT:
        return
    try:
        conf = WorkspaceClient().config
        if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
            return

        url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
        print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
        print(f"  see {url} for manual configuration", file=sys.stdout)

        os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
    except Exception:
        pass


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    if not HAS_DB_CONNECT:
        return
    try:
        with _allow_stderr_output(config):
            _enable_fallback_compute()
            if hasattr(DatabricksSession.builder, "validateSession"):
                DatabricksSession.builder.validateSession().getOrCreate()
            else:
                DatabricksSession.builder.getOrCreate()
    except Exception as e:
        print(f"Notice: Databricks Connect session could not be established ({e}); live Spark tests will skip.", file=sys.stderr)
