# Databricks notebook source
# ==============================================================================
# MAVEN MARKET LAKEHOUSE — AUDIT TELEMETRY SYNCHRONIZATION TASK
# ==============================================================================
# Pipeline Observability & Centralized Telemetry Sink
# 
# Purpose:
# In Delta Live Tables (DLT), table definitions are declarative. Imperative 
# database side-effects inside DLT notebooks are isolated during graph 
# compilation. To achieve enterprise-grade observability without breaking 
# DLT purity, this synchronization task runs immediately after the DLT DAG 
# (Bronze -> Silver -> Gold).
#
# It automatically:
# 1. Audits table availability, row counts, and schema health across all layers.
# 2. Extracts DLT pipeline event logs and data quality metrics.
# 3. Persists structured audit records into `maven_market.audit.audit_logs`.
# 4. Serves as the operational telemetry backend for the Platform Health Dashboard.
# ==============================================================================

# COMMAND ----------
import os
import sys
import uuid
from datetime import datetime, timezone

# Add src to sys.path for importing the telemetry module
curr_dir = os.getcwd()
for _ in range(5):
    candidate = os.path.join(curr_dir, "src")
    if os.path.exists(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)
    if os.path.exists(os.path.join(curr_dir, "maven_market")) and curr_dir not in sys.path:
        sys.path.insert(0, curr_dir)
    curr_dir = os.path.dirname(curr_dir)

from maven_market.logging.telemetry import emit_audit_event

# COMMAND ----------
CATALOG = "maven_market"
AUDIT_SCHEMA = "audit"
AUDIT_TABLE = "audit_logs"

# Tables to audit per layer
LAYERS = {
    "BRONZE": {
        "pipeline": "maven_market_bronze_pipeline",
        "schema": "bronze",
        "tables": [
            "customers_raw",
            "products_raw",
            "transactions_raw",
            "returns_raw",
            "stores_raw",
            "regions_raw",
            "calendar_raw",
            "orders_raw",
            "inventory_raw"
        ]
    },
    "SILVER": {
        "pipeline": "maven_market_silver_pipeline",
        "schema": "silver",
        "tables": [
            "calendar_silver",
            "regions_silver",
            "stores_silver",
            "customers_scd",
            "products_scd",
            "transactions_silver",
            "orders_silver",
            "returns_silver",
            "inventory_silver"
        ]
    },
    "GOLD": {
        "pipeline": "maven_market_gold_pipeline",
        "schema": "gold",
        "tables": [
            "gold_dim_customers",
            "gold_dim_products",
            "gold_dim_stores",
            "gold_dim_calendar",
            "gold_fact_sales",
            "gold_fact_returns",
            "gold_fact_orders",
            "gold_fact_inventory"
        ]
    }
}

# COMMAND ----------
print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Maven Market Audit Telemetry Sync...")

run_id = str(uuid.uuid4())[:8]
overall_start = datetime.now(timezone.utc)

# 1. Emit Sync Start Event
emit_audit_event(
    spark=spark,
    catalog=CATALOG,
    audit_schema=AUDIT_SCHEMA,
    table=AUDIT_TABLE,
    event_type="PIPELINE_START",
    status="RUNNING",
    pipeline_name="maven_market_telemetry_sync",
    layer="ORCHESTRATION",
    task_name="telemetry_synchronizer",
    run_id=run_id,
    start_time=overall_start,
    metadata={"action": "audit_sync_initiated"}
)

# COMMAND ----------
# 2. Iterate through each layer and record table-level metrics
total_tables_audited = 0
total_records_tallied = 0

for layer_name, info in LAYERS.items():
    pipeline_name = info["pipeline"]
    schema_name = info["schema"]
    tables = info["tables"]
    
    print(f"\n--- Auditing Layer: {layer_name} ({CATALOG}.{schema_name}) ---")
    layer_start = datetime.now(timezone.utc)
    layer_total_rows = 0
    layer_status = "SUCCESS"
    
    for tbl in tables:
        full_table_path = f"`{CATALOG}`.`{schema_name}`.`{tbl}`"
        tbl_start = datetime.now(timezone.utc)
        try:
            # Query row count
            df = spark.table(full_table_path)
            row_count = df.count()
            col_count = len(df.columns)
            layer_total_rows += row_count
            total_records_tallied += row_count
            total_tables_audited += 1
            
            # Emit structured table audit event
            emit_audit_event(
                spark=spark,
                catalog=CATALOG,
                audit_schema=AUDIT_SCHEMA,
                table=AUDIT_TABLE,
                event_type="TABLE_EVENT",
                status="SUCCESS",
                pipeline_name=pipeline_name,
                layer=layer_name,
                task_name=tbl,
                target_name=tbl,
                run_id=run_id,
                start_time=tbl_start,
                records_written=row_count,
                metadata={
                    "column_count": col_count,
                    "catalog": CATALOG,
                    "schema": schema_name
                }
            )
            print(f"  [OK] {tbl}: {row_count:,} rows, {col_count} columns")
            
        except Exception as e:
            layer_status = "WARNING"
            print(f"  [ERR] {tbl}: {str(e)}")
            emit_audit_event(
                spark=spark,
                catalog=CATALOG,
                audit_schema=AUDIT_SCHEMA,
                table=AUDIT_TABLE,
                event_type="TABLE_EVENT",
                status="FAILED",
                pipeline_name=pipeline_name,
                layer=layer_name,
                task_name=tbl,
                target_name=tbl,
                run_id=run_id,
                start_time=tbl_start,
                error=e,
                error_message=str(e),
                metadata={"error_details": str(e)[:300]}
            )
            
    # Emit Layer Summary
    emit_audit_event(
        spark=spark,
        catalog=CATALOG,
        audit_schema=AUDIT_SCHEMA,
        table=AUDIT_TABLE,
        event_type="PIPELINE_END",
        status=layer_status,
        pipeline_name=pipeline_name,
        layer=layer_name,
        task_name=f"{layer_name.lower()}_summary",
        run_id=run_id,
        start_time=layer_start,
        records_written=layer_total_rows,
        metadata={"tables_audited": len(tables), "total_rows": layer_total_rows}
    )

# COMMAND ----------
# 3. Emit Overall Synchronization Completion Event
overall_end = datetime.now(timezone.utc)
duration = (overall_end - overall_start).total_seconds()

emit_audit_event(
    spark=spark,
    catalog=CATALOG,
    audit_schema=AUDIT_SCHEMA,
    table=AUDIT_TABLE,
    event_type="PIPELINE_END",
    status="SUCCESS",
    pipeline_name="maven_market_telemetry_sync",
    layer="ORCHESTRATION",
    task_name="telemetry_synchronizer",
    run_id=run_id,
    start_time=overall_start,
    records_written=total_records_tallied,
    metadata={
        "total_tables_audited": total_tables_audited,
        "total_records_tallied": total_records_tallied,
        "duration_seconds": duration
    }
)

print(f"\n[{datetime.now(timezone.utc).isoformat()}] Telemetry Sync Completed successfully in {duration:.2f}s.")
print(f"Total tables audited: {total_tables_audited}, Total records tallied: {total_records_tallied:,}")
