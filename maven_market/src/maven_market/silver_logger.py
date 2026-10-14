"""
Custom Observability & Telemetry Logger Module for Maven Market Silver Layer.
Sinks execution metrics, quality expectation pass/fail counts, runtime durations,
and error state tracebacks into `maven_market.audit.silver_audit_logs`.
"""

import logging
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)

logger = logging.getLogger("maven_market_silver_telemetry")
logger.setLevel(logging.INFO)


def log_silver_event(
    spark: SparkSession,
    table_name: str,
    status: str,
    record_count: int = 0,
    passed_expectations: int = 0,
    failed_expectations: int = 0,
    start_time: datetime = None,
    catalog_name: str = "maven_market",
    audit_schema: str = "audit",
    error_message: str = None
):
    """
    Sinks Silver pipeline telemetry into the Unity Catalog `silver_audit_logs` table.
    """
    if start_time is None:
        start_time = datetime.now(timezone.utc)

    end_time = datetime.now(timezone.utc)
    duration_secs = int((end_time - start_time).total_seconds())

    # Ensure target audit schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{audit_schema}")

    audit_schema_spec = StructType([
        StructField("table_name", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("status", StringType(), True),
        StructField("record_count", LongType(), True),
        StructField("passed_expectations", LongType(), True),
        StructField("failed_expectations", LongType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_seconds", LongType(), True),
        StructField("error_message", StringType(), True)
    ])

    log_payload = [(
        table_name,
        "silver",
        status,
        int(record_count),
        int(passed_expectations),
        int(failed_expectations),
        start_time,
        end_time,
        duration_secs,
        error_message
    )]

    audit_df = spark.createDataFrame(log_payload, schema=audit_schema_spec)
    audit_df.write.mode("append").saveAsTable(f"{catalog_name}.{audit_schema}.silver_audit_logs")

    if status == "SUCCESS":
        logger.info(f"[SILVER_SUCCESS] {table_name}: {record_count} rows in {duration_secs}s.")
        print(f"✓ [SILVER AUDIT LOG] {table_name} -> Status: SUCCESS | Rows: {record_count} | Duration: {duration_secs}s")
    else:
        logger.error(f"[SILVER_FAILED] {table_name}: {error_message}")
        print(f"❌ [SILVER AUDIT LOG] {table_name} -> Status: FAILED | Error: {error_message}")