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
# 3. Data Generators (Consistent ID Format & Ranges)
# ==========================================
def generate_order():
    return {
        "order_id": f"ORD_{uuid.uuid4().hex[:8].upper()}",
        "store_id": random.randint(1, 24),          # Integer matching stores_raw (1-24)
        "customer_id": random.randint(1, 8488),      # Integer matching customers_raw (1-8488)
        "product_id": random.randint(1, 1560),       # Integer matching products_raw (1-1560)
        "quantity": random.randint(1, 10),
        "total_amount": round(random.uniform(5.0, 150.0), 2),
        "order_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

def generate_inventory():
    return {
        "store_id": random.randint(1, 24),          # Integer matching stores_raw (1-24)
        "product_id": random.randint(1, 1560),       # Integer matching products_raw (1-1560)
        "stock_level": random.randint(0, 100),
        "inventory_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
        print(f"Sent Order: {order_data['order_id']} (Store: {order_data['store_id']}, Prod: {order_data['product_id']}) | Sent Inventory (Store: {inventory_data['store_id']}, Prod: {inventory_data['product_id']})")
        
        time.sleep(random.uniform(1.0, 3.0))

except KeyboardInterrupt:
    print("\nData generation stopped by user.")
finally:
    producer.flush()
    print("Producer closed.")