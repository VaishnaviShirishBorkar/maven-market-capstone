# Databricks notebook source
# MAGIC %md
# MAGIC # Maven Market — Gold Layer Star Schema
# MAGIC Materialized Views creating Kimball-style Dimension and Fact tables for BI tools and SQL Dashboards.

# COMMAND ----------
import os
import sys
try:
    import dlt
except ImportError:
    from pyspark import pipelines as dlt
import yaml
from datetime import datetime, timezone
from pyspark.sql.functions import (
    col, round, coalesce, lit, concat_ws, floor, months_between, current_date, when, to_date, regexp_extract
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
    from maven_market.gold_logger import log_gold_event
    pipeline_start_time = datetime.now(timezone.utc)
    log_gold_event(
        spark=spark,
        table_name="gold_pipeline_init",
        status="SUCCESS",
        record_count=0,
        start_time=pipeline_start_time,
        catalog_name="maven_market",
        audit_schema="audit"
    )
except Exception as e:
    print(f"Logger notification skipped: {str(e)}")


# ==========================================
# 2. Config Parsing & Silver Schema Target
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
SILVER_SCHEMA = f"{catalog_name}.silver"


# ==========================================
# 3. GOLD DIMENSION TABLES (Materialized Views)
# ==========================================

@dlt.table(
    name="gold_dim_customers",
    comment="Gold customer dimension lookup table for BI reporting with integer yearly_income",
    table_properties={"quality": "gold"}
)
def gold_dim_customers():
    customers = (
        spark.read.table(f"{SILVER_SCHEMA}.customers_scd")
        .filter(col("__END_AT").isNull())
        .withColumn(
            "full_name",
            concat_ws(" ", col("first_name"), col("last_name"))
        )
        .withColumn(
            "birthdate",
            coalesce(
                to_date(col("birthdate"), "M/d/yyyy"),
                to_date(col("birthdate"), "yyyy-MM-dd")
            )
        )
        .withColumn(
            "acct_open_date",
            coalesce(
                to_date(col("acct_open_date"), "M/d/yyyy"),
                to_date(col("acct_open_date"), "yyyy-MM-dd")
            )
        )
        .withColumn(
            "age",
            floor(months_between(current_date(), col("birthdate")) / 12)
        )
        .withColumn(
            "tenure_years",
            floor(months_between(current_date(), col("acct_open_date")) / 12)
        )
        # Preserve original readable bracket string and map to integer income
        .withColumn("yearly_income_bracket", col("yearly_income"))
        .withColumn(
            "yearly_income",
            when(col("yearly_income") == "$10K - $30K", lit(20000))
            .when(col("yearly_income") == "$30K - $50K", lit(40000))
            .when(col("yearly_income") == "$50K - $70K", lit(60000))
            .when(col("yearly_income") == "$70K - $90K", lit(80000))
            .when(col("yearly_income") == "$90K - $110K", lit(100000))
            .when(col("yearly_income") == "$110K - $130K", lit(120000))
            .when(col("yearly_income") == "$130K - $150K", lit(140000))
            .when(col("yearly_income") == "$150K +", lit(150000))
            .otherwise(
                coalesce(
                    regexp_extract(col("yearly_income"), r"\$(\d+)K", 1).cast("int") * 1000,
                    col("yearly_income").cast("int"),
                    lit(0)
                )
            ).cast("integer")
        )
    )
    return customers.select(
        "customer_id", "customer_acct_num", "full_name", "first_name",
        "last_name", "gender", "marital_status", "yearly_income", "yearly_income_bracket",
        "occupation", "education", "homeowner", "customer_city",
        "customer_state_province", "customer_country", "birthdate", "age",
        "acct_open_date", "tenure_years"
    )

@dlt.table(
    name="gold_dim_products",
    comment="Gold product catalog dimension lookup table with boolean attributes",
    table_properties={"quality": "gold"}
)
def gold_dim_products():
    products = (
        spark.read.table(f"{SILVER_SCHEMA}.products_scd")
        .filter(col("__END_AT").isNull())
        .withColumn(
            "unit_profit_margin",
            round(col("product_retail_price") - col("product_cost"), 2)
        )
        .withColumn(
            "profit_margin_pct",
            when(
                col("product_retail_price") > 0,
                round(
                    ((col("product_retail_price") - col("product_cost"))
                     / col("product_retail_price")) * 100,
                    2
                )
            )
        )
        .withColumn(
            "cost_category",
            when(col("product_cost") > 5.0, "High")
            .when(col("product_cost") > 2.0, "Medium")
            .otherwise("Low")
        )
        .withColumn("recyclable", when(col("recyclable") == 1.0, lit(True)).otherwise(lit(False)))
        .withColumn("low_fat", when(col("low_fat") == 1.0, lit(True)).otherwise(lit(False)))
    )
    return products.select(
        "product_id", "product_brand", "product_name", "product_sku",
        "product_retail_price", "product_cost", "unit_profit_margin",
        "profit_margin_pct", "cost_category", "recyclable", "low_fat"
    )

@dlt.table(
    name="gold_dim_stores",
    comment="Gold store dimension table enriched with regional hierarchy and ABAC data_region",
    table_properties={"quality": "gold"}
)
def gold_dim_stores():
    stores = spark.read.table(f"{SILVER_SCHEMA}.stores_silver")
    regions = spark.read.table(f"{SILVER_SCHEMA}.regions_silver")

    return (
        stores.join(regions, "region_id", "left")
        .select(
            stores.store_id,
            stores.store_name,
            stores.store_type,
            stores.store_street_address,
            stores.store_city,
            stores.store_state,
            stores.store_country,
            stores.store_phone,
            stores.data_region,
            regions.region_id,
            regions.sales_district,
            regions.sales_region,
            stores.total_sqft,
            stores.grocery_sqft,
            stores.grocery_sqft_pct,
            stores.first_opened_date,
            stores.last_remodel_date,
            stores.years_since_remodel
        )
    )

@dlt.table(
    name="gold_dim_calendar",
    comment="Gold calendar date dimension lookup table with week_of_year",
    table_properties={"quality": "gold"}
)
def gold_dim_calendar():
    return (
        spark.read.table(f"{SILVER_SCHEMA}.calendar_silver")
        .select(
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week_of_year",
            "day_of_month",
            "day_of_week",
            "day_name",
            "is_weekend"
        )
    )


# ==========================================
# 4. GOLD FACT TABLES (Materialized Views)
# ==========================================

@dlt.table(
    name="gold_fact_sales",
    comment="Gold unified sales transaction fact table combining POS transactions and Kafka orders with data_region",
    table_properties={"quality": "gold"}
)
def gold_fact_sales():
    tx = spark.readStream.table(f"{SILVER_SCHEMA}.transactions_silver")
    orders = spark.read.table(f"{SILVER_SCHEMA}.orders_silver")

    # Combine historical POS transactions and real-time Kafka orders
    combined_sales = tx.unionByName(orders)

    return (
        combined_sales.select(
            "order_id",
            col("order_date").alias("sales_date"),
            "order_date",
            "order_timestamp",
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
            col("total_amount").alias("gross_revenue"),
            "total_amount",
            "calculated_amount",
            "total_cost",
            "gross_profit",
            "profit_margin_pct",
            "source_channel"
        )
    )

@dlt.table(
    name="gold_fact_returns",
    comment="Gold product returns fact table for return rate analytics",
    table_properties={"quality": "gold"}
)
def gold_fact_returns():
    return (
        spark.read.table(f"{SILVER_SCHEMA}.returns_silver")
        .select(
            "return_date",
            "product_id",
            "store_id",
            "quantity"
        )
    )

@dlt.table(
    name="gold_fact_orders",
    comment="Gold streaming fact table for Kafka orders and real-time order-volume analytics with data_region",
    table_properties={"quality": "gold"}
)
def gold_fact_orders():
    orders = spark.read.table(f"{SILVER_SCHEMA}.orders_silver")

    return orders.select(
        "order_id",
        "order_timestamp",
        "order_date",
        col("order_date").alias("sales_date"),
        # Dashboard can group order_timestamp by minute for order-volume KPIs.
        "product_id",
        "customer_id",
        "store_id",
        "store_name",
        "region_id",
        "data_region",
        "product_name",
        "product_brand",
        "quantity",
        "total_amount",
        col("total_amount").alias("gross_revenue"),
        "product_retail_price",
        "product_cost",
        "calculated_amount",
        "unit_profit_margin",
        "total_cost",
        "gross_profit",
        "profit_margin_pct",
        "source_channel"
    )


@dlt.table(
    name="gold_fact_inventory",
    comment="Gold current inventory stock snapshot by store and product for real-time stock levels and replenishment alerts",
    table_properties={"quality": "gold"}
)
def gold_fact_inventory():
    # Deduplicate historical observations: take the latest stock snapshot per store and product
    # so dashboard sum/count metrics reflect CURRENT inventory rather than inflating over time.
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    window_spec = Window.partitionBy("store_id", "product_id").orderBy(col("inventory_timestamp").desc())

    return (
        spark.read.table(f"{SILVER_SCHEMA}.inventory_silver")
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .select(
            "inventory_timestamp",
            to_date(col("inventory_timestamp")).alias("inventory_date"),
            "store_id",
            "store_name",
            "product_id",
            "product_name",
            "product_brand",
            "stock_level",
            "is_low_stock",
            "reorder_required"
        )
    )
