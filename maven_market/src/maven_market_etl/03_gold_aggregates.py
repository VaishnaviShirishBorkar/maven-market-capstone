# Databricks notebook source
# MAGIC %md
# MAGIC # Maven Market — Gold Layer Star Schema
# MAGIC Materialized Views creating Kimball-style Dimension and Fact tables for BI tools and SQL Dashboards.

# COMMAND ----------
import os
import sys
import dlt
import yaml
from datetime import datetime, timezone
from pyspark.sql.functions import col, round, coalesce, lit

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
    comment="Gold customer dimension lookup table for BI reporting",
    table_properties={"quality": "gold"}
)
def gold_dim_customers():
    return (
        spark.read.table(f"{SILVER_SCHEMA}.customers_silver")
        .select(
            "customer_id",
            "customer_acct_num",
            "full_name",
            "first_name",
            "last_name",
            "gender",
            "marital_status",
            "yearly_income",
            "occupation",
            "education",
            "homeowner",
            "customer_city",
            "customer_state_province",
            "customer_country",
            "birthdate",
            "age",
            "acct_open_date",
            "tenure_years"
        )
    )

@dlt.table(
    name="gold_dim_products",
    comment="Gold product catalog dimension lookup table",
    table_properties={"quality": "gold"}
)
def gold_dim_products():
    return (
        spark.read.table(f"{SILVER_SCHEMA}.products_silver")
        .select(
            "product_id",
            "product_brand",
            "product_name",
            "product_sku",
            "product_retail_price",
            "product_cost",
            "unit_profit_margin",
            "profit_margin_pct",
            "cost_category",
            "recyclable",
            "low_fat"
        )
    )

@dlt.table(
    name="gold_dim_stores",
    comment="Gold store dimension table enriched with regional hierarchy",
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
    comment="Gold calendar date dimension lookup table",
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
    comment="Gold sales transaction fact table enriched with revenue, cost, and profit measures",
    table_properties={"quality": "gold"}
)
def gold_fact_sales():
    tx = spark.read.table(f"{SILVER_SCHEMA}.transactions_silver")
    prod = spark.read.table(f"{SILVER_SCHEMA}.products_silver")

    return (
        tx.join(prod, "product_id", "left")
        .select(
            tx.transaction_date.alias("sales_date"),
            tx.stock_date,
            tx.product_id,
            tx.customer_id,
            tx.store_id,
            tx.quantity,
            prod.product_retail_price,
            prod.product_cost,
            round(tx.quantity * prod.product_retail_price, 2).alias("gross_revenue"),
            round(tx.quantity * prod.product_cost, 2).alias("total_cost"),
            round(tx.quantity * prod.unit_profit_margin, 2).alias("gross_profit")
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