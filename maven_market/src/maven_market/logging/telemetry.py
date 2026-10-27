"""Reusable structured audit logging for Maven Market Databricks pipelines.

Call emit_audit_event from orchestration/driver code, not from inside a
Lakeflow dataset definition. Dataset functions should remain declarative.
"""
from __future__ import annotations

import json
import logging
import os
import traceback as traceback_module
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, DoubleType,
)

_LOGGER_NAME = "maven_market"
logger = logging.getLogger(_LOGGER_NAME)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    ))
    logger.addHandler(handler)
logger.propagate = False

AUDIT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("run_id", StringType(), True),
    StructField("pipeline_name", StringType(), True),
    StructField("task_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("target_name", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("records_read", LongType(), True),
    StructField("records_written", LongType(), True),
    StructField("records_dropped", LongType(), True),
    StructField("error_class", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("error_traceback", StringType(), True),
    StructField("environment", StringType(), True),
    StructField("code_version", StringType(), True),
    StructField("metadata_json", StringType(), True),
])


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def emit_audit_event(
    spark: SparkSession,
    *,
    catalog: str,
    audit_schema: str = "audit",
    table: str = "audit_logs",
    event_type: str,
    status: str,
    pipeline_name: str,
    layer: str,
    run_id: Optional[str] = None,
    task_name: Optional[str] = None,
    source_name: Optional[str] = None,
    target_name: Optional[str] = None,
    start_time: Optional[datetime] = None,
    records_read: Optional[int] = None,
    records_written: Optional[int] = None,
    records_dropped: Optional[int] = None,
    error: Optional[BaseException] = None,
    error_message: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    environment: Optional[str] = None,
    code_version: Optional[str] = None,
    fallback_to_driver_logs: bool = True,
) -> None:
    """Emit one structured event to Python logs and the Unity Catalog Delta sink.

    This is an explicit driver-side write. Do not invoke it from inside a
    @dp.table/@dp.materialized_view function because those functions must only
    define DataFrames and should not perform side effects.
    """
    end_time = _utcnow()
    start_naive = start_time.replace(tzinfo=None) if start_time and start_time.tzinfo else start_time
    duration = (end_time - start_naive).total_seconds() if start_naive else None
    error_class = type(error).__name__ if error is not None else None
    error_text = error_message or (str(error) if error is not None else None)
    trace = "".join(traceback_module.format_exception(type(error), error, error.__traceback__)) if error else None
    event = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": end_time,
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "task_name": task_name,
        "layer": layer.upper(),
        "event_type": event_type.upper(),
        "status": status.upper(),
        "source_name": source_name,
        "target_name": target_name,
        "start_time": start_naive,
        "end_time": end_time,
        "duration_seconds": float(duration) if duration is not None else None,
        "records_read": int(records_read) if records_read is not None else None,
        "records_written": int(records_written) if records_written is not None else None,
        "records_dropped": int(records_dropped) if records_dropped is not None else None,
        "error_class": error_class,
        "error_message": error_text,
        "error_traceback": trace,
        "environment": environment or os.getenv("MAVEN_MARKET_ENV", "unknown"),
        "code_version": code_version or os.getenv("GIT_COMMIT_SHA"),
        "metadata_json": json.dumps(metadata or {}, default=str, sort_keys=True),
    }
    safe_log_event = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in event.items()}
    log_line = json.dumps(safe_log_event, default=str, sort_keys=True)
    (logger.error if status.upper() in {"FAILED", "ERROR"} else logger.info)(log_line)

    full_table_name = f"{catalog}.{audit_schema}.{table}"
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{audit_schema}`")
        df = spark.createDataFrame([event], schema=AUDIT_SCHEMA)
        (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table_name))
    except Exception:
        logger.exception("Unable to persist audit event to %s", full_table_name)
        if not fallback_to_driver_logs:
            raise


def log_pipeline_start(spark: SparkSession, **kwargs: Any) -> datetime:
    """Write a start event and return the start timestamp for completion logging."""
    started = _utcnow()
    emit_audit_event(spark, event_type="PIPELINE_START", status="RUNNING", start_time=started, **kwargs)
    return started


def log_pipeline_end(spark: SparkSession, *, start_time: datetime, status: str, **kwargs: Any) -> None:
    """Write a completion/failure event; pass error=... for traceback capture."""
    emit_audit_event(spark, event_type="PIPELINE_END", status=status, start_time=start_time, **kwargs)
