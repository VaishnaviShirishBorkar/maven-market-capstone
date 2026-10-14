# Databricks notebook source
# MAGIC %md
# MAGIC # Maven Market — Silver Layer Delta Live Tables Pipeline

# COMMAND ----------
import os
import sys
import dlt
import yaml
from datetime import datetime, timezone
from pyspark.sql.functions import (
    col, trim, concat_ws, coalesce, to_date, to_timestamp, year, quarter,
    month, date_format, dayofmonth, dayofweek, when, lit, round,
    floor, months_between, current_date, lead
)
from pyspark.sql.window import Window

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
    print(f"Logger notification skipped: {str(e)}")

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
# 3. Silver Streaming Dimensions (From CSV Auto Loader)
# ==========================================

@dlt.table(
    name="calendar_silver",
    comment="Cleansed calendar streaming dimension table",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("calendar", {}))
def calendar_silver():
    # FIXED: Now read as a pure stream
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.calendar_raw")
        .withColumn("date", coalesce(to_date(col("date"), "M/d/yyyy"), to_date(col("date"), "yyyy-MM-dd")))
        .filter(col("date").isNotNull())
        .withColumn("year", year(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("day_of_month", dayofmonth(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
        .withColumn("day_name", date_format(col("date"), "EEEE"))
        .withColumn("is_weekend", when(dayofweek(col("date")).isin([1, 7]), True).otherwise(False))
        .dropDuplicates(["date"])
    )


@dlt.table(
    name="regions_silver",
    comment="Cleansed sales regions master streaming dimension table",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("regions", {}))
def regions_silver():
    # FIXED: Now read as a pure stream
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.regions_raw")
        .withColumn("region_id", col("region_id").cast("integer"))
        .withColumn("sales_district", trim(col("sales_district")))
        .withColumn("sales_region", trim(col("sales_region")))
        .dropDuplicates(["region_id"])
    )


@dlt.table(
    name="stores_silver",
    comment="Cleansed store streaming dimension table",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("stores", {}))
def stores_silver():
    # FIXED: Now read as a pure stream
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
        .withColumn("first_opened_date", coalesce(to_date(col("first_opened_date"), "M/d/yyyy"), to_date(col("first_opened_date"), "yyyy-MM-dd")))
        .withColumn("last_remodel_date", coalesce(to_date(col("last_remodel_date"), "M/d/yyyy"), to_date(col("last_remodel_date"), "yyyy-MM-dd")))
        .withColumn("total_sqft", col("total_sqft").cast("integer"))
        .withColumn("grocery_sqft", col("grocery_sqft").cast("integer"))
        .withColumn("grocery_sqft_pct", round((col("grocery_sqft") / col("total_sqft")) * 100, 2))
        .withColumn("years_since_remodel", floor(months_between(current_date(), col("last_remodel_date")) / 12))
        .dropDuplicates(["store_id"])
    )


# ==========================================
# 4. Silver Batch Dimensions (From MongoDB)
# ==========================================

@dlt.table(
    name="products_silver",
    comment="Cleansed product catalog batch dimension table",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("products", {}))
def products_silver():
    # Remains BATCH (Materialized View) because source is Mongo
    return (
        spark.read.table(f"{BRONZE_SCHEMA}.products_raw")
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("product_brand", trim(col("product_brand")))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("product_sku", col("product_sku").cast("long"))
        .withColumn("product_retail_price", col("product_retail_price").cast("double"))
        .withColumn("product_cost", col("product_cost").cast("double"))
        .withColumn("product_weight", col("product_weight").cast("double"))
        .withColumn("recyclable", coalesce(col("recyclable"), lit(0.0)).cast("integer"))
        .withColumn("low_fat", coalesce(col("low_fat"), lit(0.0)).cast("integer"))
        .withColumn("unit_profit_margin", round(col("product_retail_price") - col("product_cost"), 2))
        .withColumn("profit_margin_pct", round(((col("product_retail_price") - col("product_cost")) / col("product_retail_price")) * 100, 2))
        .withColumn("cost_category", when(col("product_cost") > 5.0, "High").when(col("product_cost") > 2.0, "Medium").otherwise("Low"))
        .dropDuplicates(["product_id"])
    )


@dlt.table(
    name="customers_silver",
    comment="Current active customer batch dimension table",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("customers", {}))
def customers_silver():
    # Remains BATCH (Materialized View) because source is Mongo
    return (
        spark.read.table(f"{BRONZE_SCHEMA}.customers_raw")
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("customer_acct_num", col("customer_acct_num").cast("long"))
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("full_name", concat_ws(" ", trim(col("first_name")), trim(col("last_name"))))
        .withColumn("customer_address", trim(col("customer_address")))
        .withColumn("customer_city", trim(col("customer_city")))
        .withColumn("customer_state_province", trim(col("customer_state_province")))
        .withColumn("customer_country", trim(col("customer_country")))
        .withColumn("birthdate", coalesce(to_date(col("birthdate"), "M/d/yyyy"), to_date(col("birthdate"), "yyyy-MM-dd")))
        .withColumn("acct_open_date", coalesce(to_date(col("acct_open_date"), "M/d/yyyy"), to_date(col("acct_open_date"), "yyyy-MM-dd")))
        .withColumn("gender", trim(col("gender")))
        .withColumn("marital_status", trim(col("marital_status")))
        .withColumn("homeowner", trim(col("homeowner")))
        .withColumn("age", floor(months_between(current_date(), col("birthdate")) / 12))
        .withColumn("tenure_years", floor(months_between(current_date(), col("acct_open_date")) / 12))
        .dropDuplicates(["customer_id"])
    )


@dlt.table(
    name="customers_scd2_silver",
    comment="Historical SCD Type 2 Customer Master Data table",
    table_properties={"quality": "silver"}
)
def customers_scd2_silver():
    window_spec = Window.partitionBy("customer_id").orderBy(col("ingestion_timestamp").asc())
    
    return (
        spark.read.table(f"{BRONZE_SCHEMA}.customers_raw")
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("full_name", concat_ws(" ", trim(col("first_name")), trim(col("last_name"))))
        .withColumn("customer_address", trim(col("customer_address")))
        .withColumn("customer_city", trim(col("customer_city")))
        .withColumn("customer_state_province", trim(col("customer_state_province")))
        .withColumn("customer_country", trim(col("customer_country")))
        .withColumn("__START_AT", col("ingestion_timestamp"))
        .withColumn("__END_AT", lead(col("ingestion_timestamp"), 1).over(window_spec))
        .withColumn("__IS_CURRENT", when(col("__END_AT").isNull(), True).otherwise(False))
    )


# ==========================================
# 5. Silver Streaming Fact Tables
# ==========================================

@dlt.table(
    name="transactions_silver",
    comment="Cleansed point-of-sale transaction streaming fact table",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("transactions", {}))
def transactions_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.transactions_raw")
        .withColumn("transaction_date", coalesce(to_date(col("transaction_date"), "M/d/yyyy"), to_date(col("transaction_date"), "yyyy-MM-dd")))
        .withColumn("stock_date", coalesce(to_date(col("stock_date"), "M/d/yyyy"), to_date(col("stock_date"), "yyyy-MM-dd")))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
    )


@dlt.table(
    name="returns_silver",
    comment="Cleansed product return streaming fact table",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("returns", {}))
def returns_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.returns_raw")
        .withColumn("return_date", coalesce(to_date(col("return_date"), "M/d/yyyy"), to_date(col("return_date"), "yyyy-MM-dd")))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
    )


@dlt.table(
    name="orders_silver",
    comment="Real-time Kafka orders streaming fact table enriched with dimension metadata",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("orders", {}))
def orders_silver():
    # Uses dlt.read() to take a batch snapshot of the Silver tables for the stream-static join
    products_lookup = dlt.read("products_silver").select("product_id", "product_name", "product_brand", "product_retail_price", "unit_profit_margin")
    stores_lookup = dlt.read("stores_silver").select("store_id", "store_name", "region_id")

    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.orders_raw")
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("order_timestamp", coalesce(
            to_timestamp(col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("order_timestamp"))
        ))
        .withWatermark("order_timestamp", "10 minutes")
        .join(products_lookup, "product_id", "left")
        .join(stores_lookup, "store_id", "left")
        .withColumn("calculated_amount", round(col("quantity") * col("product_retail_price"), 2))
    )


@dlt.table(
    name="inventory_silver",
    comment="Real-time Kafka inventory streaming snapshot table enriched with dimension metadata",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("inventory", {}))
def inventory_silver():
    # Uses dlt.read() to take a batch snapshot of the Silver tables for the stream-static join
    products_lookup = dlt.read("products_silver").select("product_id", "product_name", "product_brand")
    stores_lookup = dlt.read("stores_silver").select("store_id", "store_name")

    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.inventory_raw")
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("stock_level", col("stock_level").cast("integer"))
        .withColumn("inventory_timestamp", coalesce(
            to_timestamp(col("inventory_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            to_timestamp(col("inventory_timestamp"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("inventory_timestamp"))
        ))
        .withWatermark("inventory_timestamp", "10 minutes")
        .join(products_lookup, "product_id", "left")
        .join(stores_lookup, "store_id", "left")
        .withColumn("is_low_stock", when(col("stock_level") < 10, True).otherwise(False))
        .withColumn("reorder_required", when(col("stock_level") < 10, True).otherwise(False))
    )