"""Unit tests for the Maven Market structured telemetry and audit logging module."""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Add src to sys.path
src_dir = str(Path(__file__).parent.parent / "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from maven_market.logging.telemetry import AUDIT_SCHEMA, emit_audit_event  # noqa: E402


class TestTelemetryLogging(unittest.TestCase):
    def test_audit_schema_structure(self):
        expected_fields = [
            "event_id", "event_timestamp", "run_id", "pipeline_name",
            "task_name", "layer", "event_type", "status", "source_name",
            "target_name", "start_time", "end_time", "duration_seconds",
            "records_read", "records_written", "records_dropped",
            "error_class", "error_message", "error_traceback",
            "environment", "code_version", "metadata_json"
        ]
        actual_fields = [f.name for f in AUDIT_SCHEMA.fields]
        for field in expected_fields:
            self.assertIn(field, actual_fields, f"Missing field {field} in AUDIT_SCHEMA")

    def test_emit_audit_event_fallback_when_no_spark(self):
        # When spark throws an exception or is mocked, it should gracefully fallback to python logger
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = RuntimeError("Spark cluster unavailable")

        # Must not raise with fallback_to_driver_logs=True
        try:
            emit_audit_event(
                spark=mock_spark,
                catalog="maven_market",
                audit_schema="audit",
                event_type="PIPELINE_START",
                status="RUNNING",
                pipeline_name="test_pipeline",
                layer="SILVER",
                task_name="test_task",
                fallback_to_driver_logs=True
            )
        except Exception as e:
            self.fail(f"emit_audit_event raised an unexpected exception: {e}")

    def test_error_traceback_capture(self):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = RuntimeError("Sink unavailable")

        try:
            raise ValueError("Test computation failure")
        except ValueError as err:
            caught_err = err

        # Verifies that error object is accepted and logged without crashing
        emit_audit_event(
            spark=mock_spark,
            catalog="maven_market",
            audit_schema="audit",
            event_type="PIPELINE_END",
            status="FAILED",
            pipeline_name="maven_market_silver",
            layer="SILVER",
            error=caught_err,
            fallback_to_driver_logs=True
        )


if __name__ == "__main__":
    unittest.main()
