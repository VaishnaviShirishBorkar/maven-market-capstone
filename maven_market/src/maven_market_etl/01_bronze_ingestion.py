# Databricks notebook source
%pip install pymongo

# COMMAND ----------
import os
import sys
from datetime import datetime, timezone
from pyspark import pipelines as dp
import yaml
import pymongo
from pyspark.sql.functions import current_timestamp, col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType
)

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
    from maven_market.bronze_logger import log_bronze_event
    pipeline_start_time = datetime.now(timezone.utc)
    log_bronze_event(
        spark=spark,
        table_name="bronze_pipeline_init",
        status="SUCCESS",
        record_count=0,
        start_time=pipeline_start_time,
        catalog_name="maven_market",
        audit_schema="audit"
    )
except Exception as e:
    print(f"Bronze logger notification skipped: {str(e)}")

# ==========================================
# 1. Load Configuration & Secrets
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

# Storage & MongoDB configs
raw_landing_zone = config.get("unity_catalog", {}).get("external_location_path", "")
LANDING_ZONE = raw_landing_zone.rstrip("/")

MONGO_SCOPE = config.get("mongodb", {}).get("secret_scope", "maven-market-secrets")
MONGO_KEY = config.get("mongodb", {}).get("secret_key", "mongouri")
DB_NAME = config.get("mongodb", {}).get("database_name", "maven_market")
mongo_uri = dbutils.secrets.get(scope=MONGO_SCOPE, key=MONGO_KEY)

# Kafka configs
KAFKA_BOOTSTRAP = config.get("kafka", {}).get("bootstrap_servers", "")
KAFKA_SCOPE = config.get("kafka", {}).get("secret_scope", MONGO_SCOPE)
KAFKA_KEY_NAME = config.get("kafka", {}).get("api_key_name", "confluent-api-key")
KAFKA_SECRET_NAME = config.get("kafka", {}).get("api_secret_name", "confluent-api-secret")
ORDERS_TOPIC = config.get("kafka", {}).get("orders_topic", "maven_orders")
INVENTORY_TOPIC = config.get("kafka", {}).get("inventory_topic", "maven_inventory")

kafka_api_key = dbutils.secrets.get(scope=KAFKA_SCOPE, key=KAFKA_KEY_NAME)
kafka_api_secret = dbutils.secrets.get(scope=KAFKA_SCOPE, key=KAFKA_SECRET_NAME)

jaas_config = (
    f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f"username='{kafka_api_key}' password='{kafka_api_secret}';"
)

# ==========================================
# 2. Explicit Schemas
# ==========================================

# MongoDB Schemas
customer_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_acct_num", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("customer_address", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state_province", StringType(), True),
    StructField("customer_postal_code", StringType(), True),
    StructField("customer_country", StringType(), True),
    StructField("birthdate", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("yearly_income", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("total_children", IntegerType(), True),
    StructField("num_children_at_home", IntegerType(), True),
    StructField("education", StringType(), True),
    StructField("acct_open_date", StringType(), True),
    StructField("member_card", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("homeowner", StringType(), True)
])

product_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_brand", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_sku", LongType(), True),
    StructField("product_retail_price", DoubleType(), True),
    StructField("product_cost", DoubleType(), True),
    StructField("product_weight", DoubleType(), True),
    StructField("recyclable", DoubleType(), True),
    StructField("low_fat", DoubleType(), True)
])

# POS CSV Schemas
transactions_schema = StructType([
    StructField("transaction_date", StringType(), True),
    StructField("stock_date", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True)
])

returns_schema = StructType([
    StructField("return_date", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True)
])

stores_schema = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("region_id", IntegerType(), True),
    StructField("store_type", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("store_street_address", StringType(), True),
    StructField("store_city", StringType(), True),
    StructField("store_state", StringType(), True),
    StructField("store_country", StringType(), True),
    StructField("store_phone", StringType(), True),
    StructField("first_opened_date", StringType(), True),
    StructField("last_remodel_date", StringType(), True),
    StructField("total_sqft", IntegerType(), True),
    StructField("grocery_sqft", IntegerType(), True)
])

regions_schema = StructType([
    StructField("region_id", IntegerType(), True),
    StructField("sales_district", StringType(), True),
    StructField("sales_region", StringType(), True)
])

calendar_schema = StructType([
    StructField("date", StringType(), True)
])

# Kafka Streaming Payload Schemas
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("order_timestamp", StringType(), True)
])

inventory_schema = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("stock_level", IntegerType(), True),
    StructField("inventory_timestamp", StringType(), True)
])

# ==========================================
# 3. Helpers
# ==========================================

def read_mongo_collection(collection_name: str, schema: StructType):
    client = pymongo.MongoClient(mongo_uri)
    db = client[DB_NAME]
    records = list(db[collection_name].find())
    client.close()
    
    float_fields = {"product_retail_price", "product_cost", "product_weight", "recyclable", "low_fat"}
    cleaned_records = []
    
    for r in records:
        cleaned_doc = {}
        for k, v in r.items():
            if k == "_id":
                cleaned_doc[k] = str(v)
            elif type(v).__name__ in ("Int64", "int64"):
                cleaned_doc[k] = int(v)
            elif k in float_fields and v is not None:
                cleaned_doc[k] = float(v)
            else:
                cleaned_doc[k] = v
        cleaned_records.append(cleaned_doc)
            
    df = spark.createDataFrame(cleaned_records, schema=schema)
    return df.withColumn("ingestion_timestamp", current_timestamp())


def read_pos_csv(file_pattern: str, custom_schema: StructType = None):
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
    )
    if custom_schema:
        reader = reader.schema(custom_schema)
    else:
        reader = reader.option("cloudFiles.inferColumnTypes", "true")

    return (
        reader.load(f"{LANDING_ZONE}/{file_pattern}")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )


def read_kafka_stream(topic_name: str, schema: StructType):
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .load()
    )
    
    return (
        kafka_df.selectExpr("CAST(value AS STRING) as json_payload", "topic as source_file")
        .select(from_json(col("json_payload"), schema).alias("payload"), col("source_file"))
        .select("payload.*", "source_file")
        .withColumn("ingestion_timestamp", current_timestamp())
    )

# ==========================================
# 4. DLT Bronze Ingestion Tables
# ==========================================

# --- Source 1: MongoDB Master Data (Materialized Views) ---

@dp.materialized_view(name="customers_raw", comment="Full MongoDB customer snapshot refreshed on pipeline updates", table_properties={"quality": "bronze"})
def customers_raw():
    return read_mongo_collection("customers", customer_schema)


@dp.materialized_view(name="products_raw", comment="Full MongoDB product snapshot refreshed on pipeline updates", table_properties={"quality": "bronze"})
def products_raw():
    return read_mongo_collection("products", product_schema)


# --- Source 2: Azure POS Auto Loader CSV Streams ---

@dp.table(name="transactions_raw", table_properties={"quality": "bronze"})
def transactions_raw():
    return read_pos_csv("MavenMarket_Transactions*.csv", transactions_schema)


@dp.table(name="returns_raw", table_properties={"quality": "bronze"})
def returns_raw():
    return read_pos_csv("MavenMarket_Returns*.csv", returns_schema)


@dp.table(name="stores_raw", table_properties={"quality": "bronze"})
def stores_raw():
    return read_pos_csv("MavenMarket_Stores*.csv", stores_schema)


@dp.table(name="regions_raw", table_properties={"quality": "bronze"})
def regions_raw():
    return read_pos_csv("MavenMarket_Regions*.csv", regions_schema)


@dp.table(name="calendar_raw", table_properties={"quality": "bronze"})
def calendar_raw():
    return read_pos_csv("MavenMarket_Calendar*.csv", calendar_schema)


# --- Source 3: Real-Time Kafka Event Streams ---

@dp.table(name="orders_raw", table_properties={"quality": "bronze"})
def orders_raw():
    return read_kafka_stream(ORDERS_TOPIC, orders_schema)


@dp.table(name="inventory_raw", table_properties={"quality": "bronze"})
def inventory_raw():
    return read_kafka_stream(INVENTORY_TOPIC, inventory_schema)
