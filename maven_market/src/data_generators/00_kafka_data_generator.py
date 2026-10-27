# Databricks notebook source
%pip install confluent-kafka

# COMMAND ----------
import os
import json
import time
import yaml
import random
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

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

KAFKA_BOOTSTRAP = config.get("kafka", {}).get(
    "bootstrap_servers", "pkc-w77k7w.centralus.azure.confluent.cloud:9092"
)
KAFKA_SCOPE = config.get("kafka", {}).get("secret_scope", "maven-market-secrets")
KAFKA_KEY_NAME = config.get("kafka", {}).get("api_key_name", "confluent-api-key")
KAFKA_SECRET_NAME = config.get("kafka", {}).get("api_secret_name", "confluent-api-secret")

ORDERS_TOPIC = config.get("kafka", {}).get("orders_topic", "maven_orders")
INVENTORY_TOPIC = config.get("kafka", {}).get("inventory_topic", "maven_inventory")

kafka_api_key = dbutils.secrets.get(scope=KAFKA_SCOPE, key=KAFKA_KEY_NAME)
kafka_api_secret = dbutils.secrets.get(scope=KAFKA_SCOPE, key=KAFKA_SECRET_NAME)

# ==========================================
# 2. Configure Kafka Producer
# ==========================================
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': kafka_api_key,
    'sasl.password': kafka_api_secret,
    'client.id': 'maven-market-datagen'
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback to confirm message delivery"""
    if err is not None:
        print(f"Failed to deliver message: {err}")

# ==========================================
# 3. Data Generators (Consistent ID Format, Proper Ranges & Realistic Amounts)
# ==========================================
# Toggle: True generates 1997-1998 timestamps aligning with gold_dim_calendar
#         False generates current real-time UTC timestamps
USE_HISTORICAL_DATES = True

def get_event_timestamp():
    if USE_HISTORICAL_DATES:
        year = random.choice([1997, 1998])
        month = random.randint(1, 12)
        day = random.randint(1, 28)  # Kept at 28 to safely avoid month-end calendar errors
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Cache product prices if running in Spark/Databricks environment
PRODUCT_PRICE_CACHE = {}
try:
    if "spark" in globals():
        pdf = spark.table("maven_market.bronze.products_raw").select("product_id", "product_retail_price").toPandas()
        PRODUCT_PRICE_CACHE = dict(zip(pdf["product_id"].astype(int), pdf["product_retail_price"].astype(float)))
        print(f"Loaded {len(PRODUCT_PRICE_CACHE)} product prices from products_raw.")
except Exception as e:
    print(f"Product price lookup fallback to dynamic estimation: {e}")

def get_product_price(product_id: int) -> float:
    """Return retail price from products table or compute a consistent price."""
    if product_id in PRODUCT_PRICE_CACHE and PRODUCT_PRICE_CACHE[product_id] > 0:
        return PRODUCT_PRICE_CACHE[product_id]
    # Consistent pseudo-price derived from product_id if cache not available ($0.99 to $18.99)
    return round(0.99 + ((product_id * 17) % 1800) / 100.0, 2)

def generate_order():
    store_id = random.randint(1, 24)          # Matches stores_raw (1-24)
    customer_id = random.randint(1, 10281)    # Matches customers_raw (1-10281)
    product_id = random.randint(1, 1560)      # Matches products_raw (1-1560)
    quantity = random.randint(1, 10)
    unit_price = get_product_price(product_id)
    total_amount = round(quantity * unit_price, 2)

    return {
        "order_id": f"TXN_{uuid.uuid4().hex[:8].upper()}",  # Unified TXN_ prefix matching transactions
        "store_id": store_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "total_amount": total_amount,
        "order_timestamp": get_event_timestamp()
    }

def generate_inventory():
    store_id = random.randint(1, 24)          # Matches stores_raw (1-24)
    product_id = random.randint(1, 1560)      # Matches products_raw (1-1560)
    stock_level = random.randint(0, 100)

    return {
        "store_id": store_id,
        "product_id": product_id,
        "stock_level": stock_level,
        "inventory_timestamp": get_event_timestamp()
    }

# ==========================================
# 4. Stream Loop
# ==========================================
print(f"Starting live data generation for '{ORDERS_TOPIC}' and '{INVENTORY_TOPIC}'...")
print("Interrupt or detach notebook to stop data generation.\n")

try:
    while True:
        order_data = generate_order()
        inventory_data = generate_inventory()

        # Produce Order
        producer.produce(
            topic=ORDERS_TOPIC,
            key=str(order_data["store_id"]),
            value=json.dumps(order_data),
            callback=delivery_report
        )

        # Produce Inventory
        producer.produce(
            topic=INVENTORY_TOPIC,
            key=str(inventory_data["store_id"]),
            value=json.dumps(inventory_data),
            callback=delivery_report
        )

        producer.poll(0)
        print(f"Sent Order: {order_data['order_id']} (Store: {order_data['store_id']}, Prod: {order_data['product_id']}, Amt: ${order_data['total_amount']:.2f}) | Sent Inventory (Store: {inventory_data['store_id']}, Prod: {inventory_data['product_id']}, Stock: {inventory_data['stock_level']})")
        
        time.sleep(random.uniform(1.0, 3.0))

except KeyboardInterrupt:
    print("\nData generation stopped by user.")
finally:
    producer.flush()
    print("Producer closed.")
