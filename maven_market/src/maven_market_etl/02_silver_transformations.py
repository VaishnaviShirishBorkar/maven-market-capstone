# Databricks notebook source
# MAGIC %md
# MAGIC # Maven Market — Silver Layer Delta Live Tables Pipeline

# COMMAND ----------
import os
import sys
from pyspark import pipelines as dp
import yaml
from datetime import datetime, timezone
from pyspark.sql.functions import (
    col, trim, concat, concat_ws, coalesce, to_date, to_timestamp, year, quarter,
    month, date_format, dayofmonth, dayofweek, when, lit, round,
    floor, months_between, current_date, sha2, substring, upper, weekofyear
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

@dp.table(
    name="calendar_silver",
    comment="Cleansed calendar streaming dimension table",
    table_properties={"quality": "silver"}
)
@dp.expect_all(expectations.get("calendar", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("calendar", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("calendar", {}).get("fail", {}))
def calendar_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.calendar_raw")
        .withColumn("date", coalesce(to_date(col("date"), "M/d/yyyy"), to_date(col("date"), "yyyy-MM-dd")))
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
    table_properties={"quality": "silver"}
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
    table_properties={"quality": "silver"}
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
        .withColumn("first_opened_date", coalesce(to_date(col("first_opened_date"), "M/d/yyyy"), to_date(col("first_opened_date"), "yyyy-MM-dd")))
        .withColumn("last_remodel_date", coalesce(to_date(col("last_remodel_date"), "M/d/yyyy"), to_date(col("last_remodel_date"), "yyyy-MM-dd")))
        .withColumn("total_sqft", col("total_sqft").cast("integer"))
        .withColumn("grocery_sqft", col("grocery_sqft").cast("integer"))
        .withColumn("grocery_sqft_pct", round((col("grocery_sqft") / col("total_sqft")) * 100, 2))
        .withColumn("years_since_remodel", floor(months_between(current_date(), col("last_remodel_date")) / 12))
        # ABAC Data Region Taxonomy based on store_country: USA -> NA, Canada -> EU, Mexico -> APAC
        .withColumn(
            "data_region",
            when(col("store_country") == "USA", lit("NA"))
            .when(col("store_country") == "Canada", lit("EU"))
            .when(col("store_country") == "Mexico", lit("APAC"))
            .otherwise(lit("NA"))
        )
        .dropDuplicates(["store_id"])
    )


# ==========================================
# 4. Customer and Product SCD Type 2
# ==========================================
dp.create_streaming_table(
    name="customers_scd",
    comment="SCD Type 2 customer history maintained from MongoDB snapshots",
    table_properties={"quality": "silver"}
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
        "acct_open_date", "customer_acct_num", "num_children_at_home"
    ]
)

dp.create_streaming_table(
    name="products_scd",
    comment="SCD Type 2 product history maintained from MongoDB snapshots",
    table_properties={"quality": "silver"}
)

dp.create_auto_cdc_from_snapshot_flow(
    target="products_scd",
    source=f"{BRONZE_SCHEMA}.products_raw",
    keys=["product_id"],
    stored_as_scd_type=2,
    track_history_column_list=[
        "product_brand", "product_name", "product_sku",
        "product_retail_price", "product_cost", "product_weight",
        "recyclable", "low_fat"
    ]
)


# ==========================================
# 5. Silver Streaming Fact Tables
# ==========================================

# Shared column projection ensuring transactions_silver and orders_silver follow the exact same schema
UNIFIED_SALES_COLUMNS = [
    "order_id",
    "order_timestamp",
    "order_date",
    "transaction_date",
    "stock_date",
    "product_id",
    "customer_id",
    "store_id",
    "store_name",
    "region_id",
    "data_region",
    "product_name",
    "product_brand",
    "quantity",
    "product_retail_price",
    "product_cost",
    "unit_profit_margin",
    "total_amount",
    "calculated_amount",
    "total_cost",
    "gross_profit",
    "profit_margin_pct",
    "source_channel",
]


@dp.table(
    name="transactions_silver",
    comment="Cleansed point-of-sale transaction streaming fact table enriched with order_id and product metadata",
    table_properties={"quality": "silver"}
)
@dp.expect_all(expectations.get("transactions", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("transactions", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("transactions", {}).get("fail", {}))
def transactions_silver():
    products_lookup = (
        spark.read.table("products_scd")
        .filter(col("__END_AT").isNull())
        .select(
            "product_id", "product_name", "product_brand",
            "product_retail_price", "product_cost"
        )
        .withColumn(
            "unit_profit_margin",
            round(col("product_retail_price") - col("product_cost"), 2)
        )
    )
    stores_lookup = spark.read.table("stores_silver").select("store_id", "store_name", "region_id", "data_region")

    raw_tx = (
        spark.readStream.table(f"{BRONZE_SCHEMA}.transactions_raw")
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("order_date", coalesce(
            to_date(col("transaction_date"), "M/d/yyyy"),
            to_date(col("transaction_date"), "yyyy-MM-dd")
        ))
        .withColumn("transaction_date", col("order_date"))
        .withColumn("stock_date", coalesce(
            to_date(col("stock_date"), "M/d/yyyy"),
            to_date(col("stock_date"), "yyyy-MM-dd"),
            col("order_date")
        ))
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
                                col("order_date"),
                                col("store_id"),
                                col("customer_id"),
                                col("product_id"),
                                col("quantity"),
                                coalesce(col("stock_date"), lit(""))
                            ),
                            256
                        ),
                        1,
                        8
                    )
                )
            )
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
            round(col("quantity") * (coalesce(col("product_retail_price"), lit(0.0)) - coalesce(col("product_cost"), lit(0.0))), 2)
        )
        .withColumn(
            "profit_margin_pct",
            when(
                col("total_amount") > 0,
                round((col("gross_profit") / col("total_amount")) * 100, 2)
            ).otherwise(lit(0.0))
        )
        .select(*UNIFIED_SALES_COLUMNS)
    )


@dp.table(
    name="returns_silver",
    comment="Cleansed product return streaming fact table",
    table_properties={"quality": "silver"}
)
@dp.expect_all(expectations.get("returns", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("returns", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("returns", {}).get("fail", {}))
def returns_silver():
    return (
        spark.readStream.table(f"{BRONZE_SCHEMA}.returns_raw")
        .withColumn("return_date", coalesce(to_date(col("return_date"), "M/d/yyyy"), to_date(col("return_date"), "yyyy-MM-dd")))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("store_id", col("store_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
    )


@dp.table(
    name="orders_silver",
    comment="Real-time Kafka orders streaming fact table enriched with dimension metadata",
    table_properties={"quality": "silver"}
)
@dp.expect_all(expectations.get("orders", {}).get("warn", {}))
@dp.expect_all_or_drop(expectations.get("orders", {}).get("drop", {}))
@dp.expect_all_or_fail(expectations.get("orders", {}).get("fail", {}))
def orders_silver():
    products_lookup = (
        spark.read.table("products_scd")
        .filter(col("__END_AT").isNull())
        .select(
            "product_id", "product_name", "product_brand",
            "product_retail_price", "product_cost"
        )
        .withColumn(
            "unit_profit_margin",
            round(col("product_retail_price") - col("product_cost"), 2)
        )
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
        .withColumn("order_timestamp", coalesce(
            to_timestamp(col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("order_timestamp"))
        ))
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
                round(col("quantity") * col("product_retail_price"), 2)
            ).otherwise(coalesce(round(col("total_amount"), 2), round(col("calculated_amount"), 2)))
        )
        .withColumn("total_cost", round(col("quantity") * coalesce(col("product_cost"), lit(0.0)), 2))
        .withColumn(
            "gross_profit",
            round(col("quantity") * (coalesce(col("product_retail_price"), lit(0.0)) - coalesce(col("product_cost"), lit(0.0))), 2)
        )
        .withColumn(
            "profit_margin_pct",
            when(
                col("total_amount") > 0,
                round((col("gross_profit") / col("total_amount")) * 100, 2)
            ).otherwise(lit(0.0))
        )
        .select(*UNIFIED_SALES_COLUMNS)
    )


@dp.table(
    name="inventory_silver",
    comment="Real-time Kafka inventory streaming snapshot table enriched with dimension metadata",
    table_properties={"quality": "silver"}
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