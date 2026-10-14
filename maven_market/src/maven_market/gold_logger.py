"""Backward-compatible Gold logger wrapper around shared telemetry."""
from datetime import datetime, timezone
from maven_market.logging.telemetry import emit_audit_event


def log_gold_event(spark, table_name, status, record_count=0,
                   start_time=None, catalog_name="maven_market",
                   audit_schema="audit", error_message=None, error=None,
                   run_id=None, event_type="TABLE_EVENT"):
    emit_audit_event(
        spark, catalog=catalog_name, audit_schema=audit_schema,
        event_type=event_type, status=status, pipeline_name="maven_market_gold",
        layer="GOLD", task_name=table_name, target_name=table_name,
        run_id=run_id, start_time=start_time or datetime.now(timezone.utc),
        records_written=record_count, error=error, error_message=error_message,
    )
