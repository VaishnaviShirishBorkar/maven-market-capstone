# Databricks notebook source
# MAGIC %md
# MAGIC # Maven Market — Silver Layer Delta Live Tables Pipeline
# MAGIC
# MAGIC Implements:
# MAGIC - Streaming dimension & fact tables (Bronze → Silver Medallion)
# MAGIC - SCD Type 2 for Customers and Products
# MAGIC - **Unified Quarantine Table** — captures ALL records that violate drop/fail
# MAGIC   expectations across every entity, stored as raw JSON for reprocessing.

# COMMAND ----------
import os
import sys
from pyspark import pipelines as dp
import yaml
from datetime import datetime, timezone
from pyspark.sql.functions import (
    col, trim, concat, concat_ws, coalesce, to_date, to_timestamp, year, quarter,
    month, date_format, dayofmonth, dayofweek, when, lit, round,
    floor, months_between, current_date, sha2, substring, upper, weekofyear,
    current_timestamp, array_join, array, expr, to_json, struct,
)

# ==========================================
# 1. Path Resolution & Custom Logger Import
# ==========================================
curr_dir = os.getcwd()
for _ in range(5):
    if os.path.exists(os.path.join(curr_dir, "maven_market")):
        if curr_dir not in sys.path:
            sys.path.insert(0, curr_dir)
        break
    src_path = os.path.join(curr_dir, "src")
    if os.path.exists(src_path) and os.path.exists(os.path.join(src_path, "maven_market")):
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        break
    curr_dir = os.path.dirname(curr_dir)

try:
    from maven_market.silver_logger import log_silver_event
    pipeline_start_time = datetime.now(timezone.utc)
    log_silver_event(
        spark=spark,
        table_name="silver_pipeline_init",
        status="SUCCESS",
        record_count=0,
        start_time=pipeline_start_time,
        catalog_name="maven_market",
        audit_schema="audit"
    )
except Exception as e:
    print(f"Logger notification skipped: {str(e)}")  # noqa: T201

# ==========================================
# 2. Config Parsing & Expectations Definition
# ==========================================
config = {}
curr_dir = os.getcwd()
for _ in range(5):
    candidate = os.path.join(curr_dir, "configs", "config.yml")
    if os.path.exists(candidate):
        with open(candidate, "r") as f:
            config = yaml.safe_load(f) or {}
        break
    curr_dir = os.path.dirname(curr_dir)

catalog_name = config.get("unity_catalog", {}).get("catalog_name", "maven_market")
bronze_schema = config.get("unity_catalog", {}).get("schema_name", "bronze")
BRONZE_SCHEMA = f"{catalog_name}.{bronze_schema}"

expectations = config.get("expectations", {})


# ==========================================
# QUARANTINE HELPERS
#
# Best-practice pattern: a SINGLE unified silver.quarantine streaming table
# receives rows from ALL 7 entities via dp.append_flow.
#
# Schema of quarantine:
#   quarantine_id          STRING     — UUID per bad row
#   quarantine_entity      STRING     — logical entity name (e.g. "transactions")
#   quarantine_source      STRING     — Bronze source table (e.g. "maven_market.bronze.transactions_raw")
#   quarantine_rules_violated  STRING — comma-separated names of violated rules
#   quarantine_timestamp   TIMESTAMP  — wall-clock time the violation was detected
#   raw_payload            STRING     — full row serialised as JSON for reprocessing
#
# Only drop + fail expectations are quarantine-worthy.
# warn expectations are advisory and do NOT quarantine rows.
# ==========================================

def _build_violation_filter(entity: str):
    """
    Returns (violation_col, violated_rules_col).

    violation_col       : Boolean Column — True when ≥1 drop/fail rule is violated.
    violated_rules_col  : String Column  — comma-separated names of violated rules.
    """
    drop_rules = expectations.get(entity, {}).get("drop", {})
    fail_rules = expectations.get(entity, {}).get("fail", {})
    all_rules = {**drop_rules, **fail_rules}

    if not all_rules:
        return lit(False), lit("")

    # Using 'IS NOT TRUE' ensures both explicit violations (e.g. quantity <= 0)
    # AND null violations (e.g. quantity IS NULL) evaluate to True
    violation_conditions = [expr(f"({sql_expr}) IS NOT TRUE") for sql_expr in all_rules.values()]
    combined = violation_conditions[0]
    for cond in violation_conditions[1:]:
        combined = combined | cond

    rule_flags = [
        when(expr(f"({sql_expr}) IS NOT TRUE"), lit(rule_name)).otherwise(lit(None))
        for rule_name, sql_expr in all_rules.items()
    ]
    violated_names_col = array_join(array(*rule_flags), ", ")

    return combined, violated_names_col


def _to_quarantine_row(df, entity: str, source_table: str):
    """
    Applies the violation filter to df and returns a DataFrame shaped to the
    quarantine table schema. Rows that pass all rules are excluded.
    """
    violation_col, violated_names_col = _build_violation_filter(entity)
    payload_col = to_json(struct([col(c) for c in df.columns]))
    return (
        df
        .withColumn("_violation", violation_col)
        .withColumn("_violated_rules", violated_names_col)
        .filter(col("_violation") == True)
        .select(
            expr("uuid()").alias("quarantine_id"),
            lit(entity).alias("quarantine_entity"),
            lit(source_table).alias("quarantine_source"),
            col("_violated_rules").alias("quarantine_rules_violated"),
            current_timestamp().alias("quarantine_timestamp"),
            payload_col.alias("raw_payload"),
        )
    )


# ==========================================
# 3. UNIFIED QUARANTINE STREAMING TABLE
#    Single target; seven append_flow feeds.
# ==========================================
dp.create_streaming_table(
    name="quarantine",
    comment=(
        "Unified silver-layer quarantine table. "
        "Captures every record from calendar, regions, stores, customers, products, "
        "transactions, returns, orders, and inventory that violates a drop or fail "
        "DLT expectation. Raw row is persisted as JSON for downstream investigation "
        "and reprocessing. Feeds the Platform Health Dashboard."
    ),
    table_properties={
        "quality": "quarantine",
        "pipelines.reset.allowed": "true",
    },
)


# --- calendar quarantine ---
@dp.append_flow(target="quarantine", name="calendar_quarantine_flow")
def calendar_quarantine_flow():
    source = f"{BRONZE_SCHEMA}.calendar_raw"
    raw = (
        spark.readStream.table(source)
        .withColumn(
            "date",
            coalesce(to_date(col("date"), "M/d/yyyy"), to_date(col("date"), "yyyy-MM-dd")),
        )
    )
    return _to_quarantine_row(raw, "calendar", source)


# --- regions quarantine ---
@dp.append_flow(target="quarantine", name="regions_quarantine_flow")
def regions_quarantine_flow():
    source = f"{BRONZE_SCHEMA}.regions_raw"
    raw = (
        spark.readStream.table(source)
        .withColumn("region_id", col("region_id").cast("integer"))
    )
    return _to_quarantine_row(raw, "regions", source)


# --- stores quarantine ---
@dp.append_flow(target="quarantine", name="stores_quarantine_flow")
def stores_quarantine_flow():
    source = f"{BRONZE_SCHEMA}.stores_raw"
    raw = (
        spark.readStream.table(source)
        .withColumn("store_id", col("store_id").cast("integer"))
    )
    return _to_quarantine_row(raw, "stores", source)


# --- transactions quarantine ---
@dp.append_flow(target="quarantine", name="transactions_quarantine_flow")
def transactions_quarantine_flow():
    source = f"{BRONZE_SCHEMA}.transactions_raw"
    raw = (
        spark.readStream.table(source)
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        # generate order_id so the valid_order_id rule can be evaluated
        .withColumn(
            "order_id",
            concat(
                lit("TXN_"),
                upper(
                    substring(
                        sha2(
                            concat_ws(
                                "_",
                                col("transaction_date"),
                                col("store_id"),
                                col("customer_id"),
                                col("product_id"),
                                col("quantity"),
                            ),
                            256,
                        ),
                        1,
                        8,
                    )
                ),
            ),
        )
    )
    return _to_quarantine_row(raw, "transactions", source)


# --- returns quarantine ---
@dp.append_flow(target="quarantine", name="returns_quarantine_flow")
def returns_quarantine_flow():
    source = f"{BRONZE_SCHEMA}.returns_raw"
    raw = (
        spark.readStream.table(source)
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
    )
    return _to_quarantine_row(raw, "returns", source)


# --- orders quarantine ---
@dp.append_flow(target="quarantine", name="orders_quarantine_flow")
def orders_quarantine_flow():
    source = f"{BRONZE_SCHEMA}.orders_raw"
    raw = (
        spark.readStream.table(source)
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("total_amount", col("total_amount").cast("double"))
    )
    return _to_quarantine_row(raw, "orders", source)


# --- inventory quarantine ---
@dp.append_flow(target="quarantine", name="inventory_quarantine_flow")
def inventory_quarantine_flow():
    source = f"{BRONZE_SCHEMA}.inventory_raw"
    raw = (
        spark.readStream.table(source)
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("stock_level", col("stock_level").cast("integer"))
    )
    return _to_quarantine_row(raw, "inventory", source)


# ==========================================
# 4. Silver Streaming Dimensions (From CSV Auto Loader)
# ==========================================

@dp.table(
    name="calendar_silver",
    comment="Cleansed calendar streaming dimension table",
    table_properties={"quality": "silver"},
)
@dp.expect_all(expectations.get("calendar", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("calendar", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("calendar", {}).get("fail", {}))
def calendar_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.calendar_raw")
        .withColumn(
            "date",
            coalesce(to_date(col("date"), "M/d/yyyy"), to_date(col("date"), "yyyy-MM-dd")),
        )
        .filter(col("date").isNotNull())
        .withColumn("year", year(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("week_of_year", weekofyear(col("date")))
        .withColumn("day_of_month", dayofmonth(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
        .withColumn("day_name", date_format(col("date"), "EEEE"))
        .withColumn("is_weekend", when(dayofweek(col("date")).isin([1, 7]), True).otherwise(False))
        .dropDuplicates(["date"])
    )


@dp.table(
    name="regions_silver",
    comment="Cleansed sales regions master streaming dimension table",
    table_properties={"quality": "silver"},
)
@dp.expect_all(expectations.get("regions", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("regions", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("regions", {}).get("fail", {}))
def regions_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.regions_raw")
        .withColumn("region_id", col("region_id").cast("integer"))
        .withColumn("sales_district", trim(col("sales_district")))
        .withColumn("sales_region", trim(col("sales_region")))
        .dropDuplicates(["region_id"])
    )


@dp.table(
    name="stores_silver",
    comment="Cleansed store streaming dimension table with ABAC data_region taxonomy",
    table_properties={"quality": "silver"},
)
@dp.expect_all(expectations.get("stores", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("stores", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("stores", {}).get("fail", {}))
def stores_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.stores_raw")
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("region_id", col("region_id").cast("integer"))
        .withColumn("store_type", trim(col("store_type")))
        .withColumn("store_name", trim(col("store_name")))
        .withColumn("store_street_address", trim(col("store_street_address")))
        .withColumn("store_city", trim(col("store_city")))
        .withColumn("store_state", trim(col("store_state")))
        .withColumn("store_country", trim(col("store_country")))
        .withColumn("store_phone", trim(col("store_phone")))
        .withColumn(
            "first_opened_date",
            coalesce(
                to_date(col("first_opened_date"), "M/d/yyyy"),
                to_date(col("first_opened_date"), "yyyy-MM-dd"),
            ),
        )
        .withColumn(
            "last_remodel_date",
            coalesce(
                to_date(col("last_remodel_date"), "M/d/yyyy"),
                to_date(col("last_remodel_date"), "yyyy-MM-dd"),
            ),
        )
        .withColumn("total_sqft", col("total_sqft").cast("integer"))
        .withColumn("grocery_sqft", col("grocery_sqft").cast("integer"))
        .withColumn("grocery_sqft_pct", round((col("grocery_sqft") / col("total_sqft")) * 100, 2))
        .withColumn(
            "years_since_remodel",
            floor(months_between(current_date(), col("last_remodel_date")) / 12),
        )
        # ABAC Data Region Taxonomy: USA → NA, Canada → EU, Mexico → APAC
        .withColumn(
            "data_region",
            when(col("store_country") == "USA", lit("NA"))
            .when(col("store_country") == "Canada", lit("EU"))
            .when(col("store_country") == "Mexico", lit("APAC"))
            .otherwise(lit("NA")),
        )
        .dropDuplicates(["store_id"])
    )


# ==========================================
# 5. Customer and Product SCD Type 2
# ==========================================
dp.create_streaming_table(
    name="customers_scd",
    comment="SCD Type 2 customer history maintained from MongoDB snapshots",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_from_snapshot_flow(
    target="customers_scd",
    source=f"{BRONZE_SCHEMA}.customers_raw",
    keys=["customer_id"],
    stored_as_scd_type=2,
    track_history_column_list=[
        "first_name", "last_name", "customer_address", "customer_city",
        "customer_state_province", "customer_postal_code", "customer_country",
        "birthdate", "marital_status", "yearly_income", "total_children",
        "education", "gender", "homeowner", "member_card", "occupation",
        "acct_open_date", "customer_acct_num", "num_children_at_home",
    ],
)

dp.create_streaming_table(
    name="products_scd",
    comment="SCD Type 2 product history maintained from MongoDB snapshots",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_from_snapshot_flow(
    target="products_scd",
    source=f"{BRONZE_SCHEMA}.products_raw",
    keys=["product_id"],
    stored_as_scd_type=2,
    track_history_column_list=[
        "product_brand", "product_name", "product_sku",
        "product_retail_price", "product_cost", "product_weight",
        "recyclable", "low_fat",
    ],
)


# ==========================================
# 6. Silver Streaming Fact Tables
# ==========================================

# Shared column projection — transactions_silver & orders_silver must match exactly
UNIFIED_SALES_COLUMNS = [
    "order_id", "order_timestamp", "order_date", "transaction_date", "stock_date",
    "product_id", "customer_id", "store_id", "store_name", "region_id", "data_region",
    "product_name", "product_brand", "quantity", "product_retail_price", "product_cost",
    "unit_profit_margin", "total_amount", "calculated_amount", "total_cost",
    "gross_profit", "profit_margin_pct", "source_channel",
]


@dp.table(
    name="transactions_silver",
    comment="Cleansed point-of-sale transaction streaming fact table enriched with order_id and product metadata",
    table_properties={"quality": "silver"},
)
@dp.expect_all(expectations.get("transactions", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("transactions", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("transactions", {}).get("fail", {}))
def transactions_silver():
    products_lookup = (
        spark.read.table("products_scd")
        .filter(col("__END_AT").isNull())
        .select("product_id", "product_name", "product_brand", "product_retail_price", "product_cost")
        .withColumn("unit_profit_margin", round(col("product_retail_price") - col("product_cost"), 2))
    )
    stores_lookup = spark.read.table("stores_silver").select("store_id", "store_name", "region_id", "data_region")

    raw_tx = (
        spark.readStream.table(f"{BRONZE_SCHEMA}.transactions_raw")
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn(
            "order_date",
            coalesce(
                to_date(col("transaction_date"), "M/d/yyyy"),
                to_date(col("transaction_date"), "yyyy-MM-dd"),
            ),
        )
        .withColumn("transaction_date", col("order_date"))
        .withColumn(
            "stock_date",
            coalesce(
                to_date(col("stock_date"), "M/d/yyyy"),
                to_date(col("stock_date"), "yyyy-MM-dd"),
                col("order_date"),
            ),
        )
        .withColumn("order_timestamp", to_timestamp(col("order_date")))
        .withColumn("source_channel", lit("POS"))
        .withColumn(
            "order_id",
            concat(
                lit("TXN_"),
                upper(
                    substring(
                        sha2(
                            concat_ws(
                                "_",
                                col("order_date"), col("store_id"), col("customer_id"),
                                col("product_id"), col("quantity"),
                                coalesce(col("stock_date"), lit("")),
                            ),
                            256,
                        ),
                        1,
                        8,
                    )
                ),
            ),
        )
    )

    return (
        raw_tx
        .join(products_lookup, "product_id", "left")
        .join(stores_lookup, "store_id", "left")
        .withColumn("calculated_amount", round(col("quantity") * coalesce(col("product_retail_price"), lit(0.0)), 2))
        .withColumn("total_amount", round(col("quantity") * coalesce(col("product_retail_price"), lit(0.0)), 2))
        .withColumn("total_cost", round(col("quantity") * coalesce(col("product_cost"), lit(0.0)), 2))
        .withColumn(
            "gross_profit",
            round(
                col("quantity") * (
                    coalesce(col("product_retail_price"), lit(0.0))
                    - coalesce(col("product_cost"), lit(0.0))
                ),
                2,
            ),
        )
        .withColumn(
            "profit_margin_pct",
            when(col("total_amount") > 0, round((col("gross_profit") / col("total_amount")) * 100, 2))
            .otherwise(lit(0.0)),
        )
        .select(*UNIFIED_SALES_COLUMNS)
    )


@dp.table(
    name="returns_silver",
    comment="Cleansed product return streaming fact table",
    table_properties={"quality": "silver"},
)
@dp.expect_all(expectations.get("returns", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("returns", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("returns", {}).get("fail", {}))
def returns_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.returns_raw")
        .withColumn(
            "return_date",
            coalesce(
                to_date(col("return_date"), "M/d/yyyy"),
                to_date(col("return_date"), "yyyy-MM-dd"),
            ),
        )
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
    )


@dp.table(
    name="orders_silver",
    comment="Real-time Kafka orders streaming fact table enriched with dimension metadata",
    table_properties={"quality": "silver"},
)
@dp.expect_all(expectations.get("orders", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("orders", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("orders", {}).get("fail", {}))
def orders_silver():
    products_lookup = (
        spark.read.table("products_scd")
        .filter(col("__END_AT").isNull())
        .select("product_id", "product_name", "product_brand", "product_retail_price", "product_cost")
        .withColumn("unit_profit_margin", round(col("product_retail_price") - col("product_cost"), 2))
    )
    stores_lookup = spark.read.table("stores_silver").select("store_id", "store_name", "region_id", "data_region")

    raw_orders = (
        spark.readStream.table(f"{BRONZE_SCHEMA}.orders_raw")
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn(
            "order_timestamp",
            coalesce(
                to_timestamp(col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("order_timestamp")),
            ),
        )
        .withWatermark("order_timestamp", "10 minutes")
        .withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("transaction_date", to_date(col("order_timestamp")))
        .withColumn("stock_date", to_date(col("order_timestamp")))
        .withColumn("source_channel", lit("KAFKA"))
    )

    return (
        raw_orders
        .join(products_lookup, "product_id", "left")
        .join(stores_lookup, "store_id", "left")
        .withColumn("calculated_amount", round(col("quantity") * coalesce(col("product_retail_price"), lit(0.0)), 2))
        .withColumn(
            "total_amount",
            when(
                coalesce(col("product_retail_price"), lit(0.0)) > 0,
                round(col("quantity") * col("product_retail_price"), 2),
            ).otherwise(coalesce(round(col("total_amount"), 2), round(col("calculated_amount"), 2))),
        )
        .withColumn("total_cost", round(col("quantity") * coalesce(col("product_cost"), lit(0.0)), 2))
        .withColumn(
            "gross_profit",
            round(
                col("quantity") * (
                    coalesce(col("product_retail_price"), lit(0.0))
                    - coalesce(col("product_cost"), lit(0.0))
                ),
                2,
            ),
        )
        .withColumn(
            "profit_margin_pct",
            when(col("total_amount") > 0, round((col("gross_profit") / col("total_amount")) * 100, 2))
            .otherwise(lit(0.0)),
        )
        .select(*UNIFIED_SALES_COLUMNS)
    )


@dp.table(
    name="inventory_silver",
    comment="Real-time Kafka inventory streaming snapshot table enriched with dimension metadata",
    table_properties={"quality": "silver"},
)
@dp.expect_all(expectations.get("inventory", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("inventory", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("inventory", {}).get("fail", {}))
def inventory_silver():
    products_lookup = (
        spark.read.table("products_scd")
        .filter(col("__END_AT").isNull())
        .select("product_id", "product_name", "product_brand")
    )
    stores_lookup = spark.read.table("stores_silver").select("store_id", "store_name")

    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.inventory_raw")
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("stock_level", col("stock_level").cast("integer"))
        .withColumn(
            "inventory_timestamp",
            coalesce(
                to_timestamp(col("inventory_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col("inventory_timestamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("inventory_timestamp")),
            ),
        )
        .withWatermark("inventory_timestamp", "10 minutes")
        .join(products_lookup, "product_id", "left")
        .join(stores_lookup, "store_id", "left")
        .withColumn("is_low_stock", when(col("stock_level") < 10, True).otherwise(False))
        .withColumn("reorder_required", when(col("stock_level") < 10, True).otherwise(False))
    )
