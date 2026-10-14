# Databricks notebook source
# ==============================================================================
# Pipeline: Maven Market — Silver Layer Delta Live Tables Pipeline
# Script:   02_silver_transformations.py
# Purpose:  Cleanses raw bronze data, tracks historical mutations via 
#           AUTO CDC FROM SNAPSHOT for master data (Customers & Products),
#           executes stream-static joins, and enforces dynamic DLT expectations.
# ==============================================================================

import os
import sys
import yaml
import dlt
from datetime import datetime, timezone
from pyspark.sql.functions import (
    col, trim, concat_ws, coalesce, to_date, to_timestamp, year, quarter,
    month, date_format, dayofmonth, dayofweek, when, lit, round,
    floor, months_between, current_date, current_timestamp
)

# Lakeflow / DLT API aliasing
try:
    from pyspark import pipelines as dp
except ImportError:
    dp = dlt

# ==============================================================================
# 1. Config Parsing & Expectations Definition
# ==============================================================================
curr_dir = os.getcwd()
config = {}
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

# Dynamic quality expectations dictionary from config.yml
expectations = config.get("expectations", {})


# ==============================================================================
# 2. Reusable Cleansing Helpers
# ==============================================================================
def parse_flexible_date(col_name: str):
    """Parses mixed string date formats (M/d/yyyy and yyyy-MM-dd) into DateType."""
    return coalesce(
        to_date(col(col_name), "M/d/yyyy"),
        to_date(col(col_name), "yyyy-MM-dd"),
        to_date(col(col_name), "MM/dd/yyyy"),
        to_date(col(col_name))
    )

def parse_flexible_timestamp(col_name: str):
    """Parses ISO-8601 and standard formatted string timestamps into TimestampType."""
    return coalesce(
        to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col(col_name))
    )


# ==============================================================================
# 3. Operational Streaming Dimensions (From Auto Loader CSV Sources)
# ==============================================================================

@dlt.table(
    name="calendar_silver",
    comment="Cleansed calendar dimension enriched with derived calendar hierarchies",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("calendar", {}))
def calendar_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.calendar_raw")
        .withColumn("date", parse_flexible_date("date"))
        .filter(col("date").isNotNull())
        .withColumn("year", year(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("day_of_month", dayofmonth(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
        .withColumn("day_name", date_format(col("date"), "EEEE"))
        .withColumn("is_weekend", when(dayofweek(col("date")).isin([1, 7]), True).otherwise(False))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .dropDuplicates(["date"])
    )


@dlt.table(
    name="regions_silver",
    comment="Cleansed sales regions master dimension table",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("regions", {}))
def regions_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.regions_raw")
        .withColumn("region_id", col("region_id").cast("integer"))
        .withColumn("sales_district", trim(col("sales_district")))
        .withColumn("sales_region", trim(col("sales_region")))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .dropDuplicates(["region_id"])
    )


@dlt.table(
    name="stores_silver",
    comment="Cleansed store master dimension with square footage ratios and remodel age",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("stores", {}))
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
        .withColumn("first_opened_date", parse_flexible_date("first_opened_date"))
        .withColumn("last_remodel_date", parse_flexible_date("last_remodel_date"))
        .withColumn("total_sqft", col("total_sqft").cast("integer"))
        .withColumn("grocery_sqft", col("grocery_sqft").cast("integer"))
        .withColumn("grocery_sqft_pct", round((col("grocery_sqft") / col("total_sqft")) * 100, 2))
        .withColumn("years_since_remodel", floor(months_between(current_date(), col("last_remodel_date")) / 12))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .dropDuplicates(["store_id"])
    )


# ==============================================================================
# 4. Master Data: Current State + AUTO CDC FROM SNAPSHOT (MongoDB Sources)
# ==============================================================================

# ------------------------------------------------------------------------------
# 4A. PRODUCTS MASTER DATA
# ------------------------------------------------------------------------------

@dlt.table(
    name="products_silver",
    comment="Current active product catalog dimension with margins and cost classification",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("products", {}))
def products_silver():
    """Current active snapshot: 1 deduplicated record per product_id."""
    return (
        spark.read.table(f"{BRONZE_SCHEMA}.products_raw")
        .select(
            col("product_id").cast("integer").alias("product_id"),
            trim(col("product_brand")).alias("product_brand"),
            trim(col("product_name")).alias("product_name"),
            col("product_sku").cast("long").alias("product_sku"),
            col("product_retail_price").cast("double").alias("product_retail_price"),
            col("product_cost").cast("double").alias("product_cost"),
            col("product_weight").cast("double").alias("product_weight"),
            coalesce(col("recyclable"), lit(0.0)).cast("integer").alias("recyclable"),
            coalesce(col("low_fat"), lit(0.0)).cast("integer").alias("low_fat")
        )
        .withColumn("unit_profit_margin", round(col("product_retail_price") - col("product_cost"), 2))
        .withColumn("profit_margin_pct", round(((col("product_retail_price") - col("product_cost")) / col("product_retail_price")) * 100, 2))
        .withColumn("cost_category", when(col("product_cost") > 5.0, "High").when(col("product_cost") > 2.0, "Medium").otherwise("Low"))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .dropDuplicates(["product_id"])
    )


@dlt.view(name="products_snapshot_v")
@dlt.expect_all_or_drop(expectations.get("products", {}))
def products_snapshot_v():
    """
    Cleansed batch view feeding the Snapshot CDC engine.
    Explicitly projects only business attributes to avoid false diff triggers from metadata.
    """
    return (
        spark.read.table(f"{BRONZE_SCHEMA}.products_raw")
        .select(
            col("product_id").cast("integer").alias("product_id"),
            trim(col("product_brand")).alias("product_brand"),
            trim(col("product_name")).alias("product_name"),
            col("product_sku").cast("long").alias("product_sku"),
            col("product_retail_price").cast("double").alias("product_retail_price"),
            col("product_cost").cast("double").alias("product_cost"),
            col("product_weight").cast("double").alias("product_weight"),
            coalesce(col("recyclable"), lit(0.0)).cast("integer").alias("recyclable"),
            coalesce(col("low_fat"), lit(0.0)).cast("integer").alias("low_fat")
        )
        .dropDuplicates(["product_id"])
    )


dp.create_streaming_table(
    name="products_scd2_silver",
    comment="Historical SCD Type 2 Product Master Data tracked via snapshot diffs",
    table_properties={"quality": "silver"}
)

# product_id is the primary key; SCD2 is applied across all non-key descriptive attributes
if hasattr(dp, "create_auto_cdc_from_snapshot_flow"):
    dp.create_auto_cdc_from_snapshot_flow(
        target="products_scd2_silver",
        source="products_snapshot_v",
        keys=["product_id"],
        stored_as_scd_type=2
    )
else:
    dlt.apply_changes_from_snapshot(
        target="products_scd2_silver",
        source="products_snapshot_v",
        keys=["product_id"],
        stored_as_scd_type=2
    )


# ------------------------------------------------------------------------------
# 4B. CUSTOMERS MASTER DATA
# ------------------------------------------------------------------------------

@dlt.table(
    name="customers_silver",
    comment="Current active customer dimension table with demographic calculations",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("customers", {}))
def customers_silver():
    """Current active snapshot: 1 deduplicated record per customer_id."""
    return (
        spark.read.table(f"{BRONZE_SCHEMA}.customers_raw")
        .select(
            col("customer_id").cast("integer").alias("customer_id"),
            col("customer_acct_num").cast("long").alias("customer_acct_num"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            concat_ws(" ", trim(col("first_name")), trim(col("last_name"))).alias("full_name"),
            trim(col("customer_address")).alias("customer_address"),
            trim(col("customer_city")).alias("customer_city"),
            trim(col("customer_state_province")).alias("customer_state_province"),
            trim(col("customer_postal_code")).alias("customer_postal_code"),
            trim(col("customer_country")).alias("customer_country"),
            parse_flexible_date("birthdate").alias("birthdate"),
            parse_flexible_date("acct_open_date").alias("acct_open_date"),
            trim(col("gender")).alias("gender"),
            trim(col("marital_status")).alias("marital_status"),
            trim(col("yearly_income")).alias("yearly_income"),
            col("total_children").cast("integer").alias("total_children"),
            col("num_children_at_home").cast("integer").alias("num_children_at_home"),
            trim(col("education")).alias("education"),
            trim(col("member_card")).alias("member_card"),
            trim(col("occupation")).alias("occupation"),
            trim(col("homeowner")).alias("homeowner")
        )
        .withColumn("age", floor(months_between(current_date(), col("birthdate")) / 12))
        .withColumn("tenure_years", floor(months_between(current_date(), col("acct_open_date")) / 12))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .dropDuplicates(["customer_id"])
    )


@dlt.view(name="customers_snapshot_v")
@dlt.expect_all_or_drop(expectations.get("customers", {}))
def customers_snapshot_v():
    """
    Cleansed batch view feeding the Snapshot CDC engine.
    Explicitly projects only business attributes to avoid false diff triggers from metadata.
    """
    return (
        spark.read.table(f"{BRONZE_SCHEMA}.customers_raw")
        .select(
            col("customer_id").cast("integer").alias("customer_id"),
            col("customer_acct_num").cast("long").alias("customer_acct_num"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            concat_ws(" ", trim(col("first_name")), trim(col("last_name"))).alias("full_name"),
            trim(col("customer_address")).alias("customer_address"),
            trim(col("customer_city")).alias("customer_city"),
            trim(col("customer_state_province")).alias("customer_state_province"),
            trim(col("customer_postal_code")).alias("customer_postal_code"),
            trim(col("customer_country")).alias("customer_country"),
            parse_flexible_date("birthdate").alias("birthdate"),
            parse_flexible_date("acct_open_date").alias("acct_open_date"),
            trim(col("gender")).alias("gender"),
            trim(col("marital_status")).alias("marital_status"),
            trim(col("yearly_income")).alias("yearly_income"),
            col("total_children").cast("integer").alias("total_children"),
            col("num_children_at_home").cast("integer").alias("num_children_at_home"),
            trim(col("education")).alias("education"),
            trim(col("member_card")).alias("member_card"),
            trim(col("occupation")).alias("occupation"),
            trim(col("homeowner")).alias("homeowner")
        )
        .dropDuplicates(["customer_id"])
    )


dp.create_streaming_table(
    name="customers_scd2_silver",
    comment="Historical SCD Type 2 Customer Master Data tracked via snapshot diffs",
    table_properties={"quality": "silver"}
)

# customer_id is the primary key; SCD2 is applied across all non-key descriptive attributes
if hasattr(dp, "create_auto_cdc_from_snapshot_flow"):
    dp.create_auto_cdc_from_snapshot_flow(
        target="customers_scd2_silver",
        source="customers_snapshot_v",
        keys=["customer_id"],
        stored_as_scd_type=2
    )
else:
    dlt.apply_changes_from_snapshot(
        target="customers_scd2_silver",
        source="customers_snapshot_v",
        keys=["customer_id"],
        stored_as_scd_type=2
    )


# ==============================================================================
# 5. Cleansed POS Streaming Facts (From Auto Loader CSV Sources)
# ==============================================================================

@dlt.table(
    name="transactions_silver",
    comment="Cleansed point-of-sale transaction fact table with validated quantities",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("transactions", {}))
def transactions_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.transactions_raw")
        .withColumn("transaction_date", parse_flexible_date("transaction_date"))
        .withColumn("stock_date", parse_flexible_date("stock_date"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("silver_processed_timestamp", current_timestamp())
    )


@dlt.table(
    name="returns_silver",
    comment="Cleansed product returns fact table with validated return volumes",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("returns", {}))
def returns_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.returns_raw")
        .withColumn("return_date", parse_flexible_date("return_date"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("silver_processed_timestamp", current_timestamp())
    )


# ==============================================================================
# 6. Stream-Static Enriched Fact Tables with Watermarking (Real-Time Kafka)
# ==============================================================================

@dlt.table(
    name="orders_silver",
    comment="Real-time Kafka orders enriched with store, customer, and product metadata",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("orders", {}))
def orders_silver():
    products_lookup = dlt.read("products_silver").select(
        "product_id", "product_name", "product_brand", "product_retail_price", "unit_profit_margin"
    )
    stores_lookup = dlt.read("stores_silver").select(
        "store_id", "store_name", "region_id"
    )
    customers_lookup = dlt.read("customers_silver").select(
        "customer_id", "full_name", "customer_country", "member_card"
    )

    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.orders_raw")
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("order_timestamp", parse_flexible_timestamp("order_timestamp"))
        .withWatermark("order_timestamp", "10 minutes")
        .join(products_lookup, on="product_id", how="left")
        .join(stores_lookup, on="store_id", how="left")
        .join(customers_lookup, on="customer_id", how="left")
        .withColumn("calculated_amount", round(col("quantity") * col("product_retail_price"), 2))
        .withColumn("silver_processed_timestamp", current_timestamp())
    )


@dlt.table(
    name="inventory_silver",
    comment="Real-time Kafka inventory snapshots enriched with product/store metadata and alerts",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(expectations.get("inventory", {}))
def inventory_silver():
    products_lookup = dlt.read("products_silver").select(
        "product_id", "product_name", "product_brand"
    )
    stores_lookup = dlt.read("stores_silver").select(
        "store_id", "store_name", "region_id"
    )

    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.inventory_raw")
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("stock_level", col("stock_level").cast("integer"))
        .withColumn("inventory_timestamp", parse_flexible_timestamp("inventory_timestamp"))
        .withWatermark("inventory_timestamp", "10 minutes")
        .join(products_lookup, on="product_id", how="left")
        .join(stores_lookup, on="store_id", how="left")
        .withColumn("is_low_stock", when(col("stock_level") < 10, True).otherwise(False))
        .withColumn("reorder_required", when(col("stock_level") < 10, True).otherwise(False))
        .withColumn("silver_processed_timestamp", current_timestamp())
    )