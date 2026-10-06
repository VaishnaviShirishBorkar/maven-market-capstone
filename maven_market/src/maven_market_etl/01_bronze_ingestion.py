# src/maven_market_etl/01_bronze_ingestion.py

import os
import dlt
import yaml
from pyspark.sql.functions import current_timestamp, input_file_name

# ==========================================
# 1. Load Configuration
# ==========================================
config_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../configs/config.yml")
)
with open(config_path, "r") as f:
  config = yaml.safe_load(f)

# Extract config variables
SECRET_SCOPE = config["mongodb"]["secret_scope"]
SECRET_KEY = config["mongodb"]["secret_key"]
DB_NAME = config["mongodb"]["database_name"]

# Safely extract landing zone path and strip trailing slash to avoid double slashes in ABFSS URLs
LANDING_ZONE = (
    config.get("unity_catalog", {}).get("external_location_path")
    or config.get("azure_storage", {}).get("landing_zone_path")
).rstrip("/")

# Retrieve MongoDB URI securely from secret scope
mongo_uri = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)

# ==========================================
# 2. MongoDB Master Data Ingestion
# ==========================================


@dlt.table(
    name="customers_raw",
    comment="Raw customer master data read from MongoDB Atlas",
    table_properties={"quality": "bronze"},
)
def customers_raw():
  return (
      spark.read.format("mongodb")
      .option("spark.mongodb.read.connection.uri", mongo_uri)
      .option("spark.mongodb.read.database", DB_NAME)
      .option("spark.mongodb.read.collection", "customers")
      .load()
      .withColumn("ingestion_timestamp", current_timestamp())
  )


@dlt.table(
    name="products_raw",
    comment="Raw product master data read from MongoDB Atlas",
    table_properties={"quality": "bronze"},
)
def products_raw():
  return (
      spark.read.format("mongodb")
      .option("spark.mongodb.read.connection.uri", mongo_uri)
      .option("spark.mongodb.read.database", DB_NAME)
      .option("spark.mongodb.read.collection", "products")
      .load()
      .withColumn("ingestion_timestamp", current_timestamp())
  )


# ==========================================
# 3. Azure POS CSVs Auto Loader Ingestion (Direct ABFSS)
# ==========================================


@dlt.table(
    name="transactions_raw",
    comment="Raw POS transactions ingested via Auto Loader",
    table_properties={"quality": "bronze"},
)
def transactions_raw():
  return (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{LANDING_ZONE}/MavenMarket_Transactions*.csv")
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
  )


@dlt.table(
    name="returns_raw",
    comment="Raw product returns ingested via Auto Loader",
    table_properties={"quality": "bronze"},
)
def returns_raw():
  return (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{LANDING_ZONE}/MavenMarket_Returns*.csv")
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
  )


@dlt.table(
    name="stores_raw",
    comment="Raw store operational data",
    table_properties={"quality": "bronze"},
)
def stores_raw():
  return (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{LANDING_ZONE}/MavenMarket_Stores.csv")
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
  )


@dlt.table(
    name="regions_raw",
    comment="Raw regional organizational data",
    table_properties={"quality": "bronze"},
)
def regions_raw():
  return (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{LANDING_ZONE}/MavenMarket_Regions.csv")
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
  )


@dlt.table(
    name="calendar_raw",
    comment="Raw calendar dimension data",
    table_properties={"quality": "bronze"},
)
def calendar_raw():
  return (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{LANDING_ZONE}/MavenMarket_Calendar.csv")
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
  )