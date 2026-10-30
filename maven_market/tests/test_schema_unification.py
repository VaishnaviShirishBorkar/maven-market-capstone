import hashlib
import unittest
import yaml
from pathlib import Path


class TestSchemaUnification(unittest.TestCase):
    # def test_config_expectations_aligned(self):
    #     config_path = Path(__file__).parent.parent / "configs" / "config.yml"
    #     with open(config_path, "r") as f:
    #         config = yaml.safe_load(f)

    #     exp = config.get("expectations", {})
    #     tx_exp = exp.get("transactions", {})
    #     order_exp = exp.get("orders", {})

    #     self.assertIn("valid_order_id", tx_exp)
    #     self.assertIn("valid_order_id", order_exp)
    #     self.assertEqual(tx_exp["valid_order_id"], "order_id IS NOT NULL")
    #     self.assertEqual(order_exp["valid_order_id"], "order_id IS NOT NULL")

    #     self.assertEqual(tx_exp["valid_quantity"], "quantity > 0")
    #     self.assertEqual(order_exp["valid_quantity"], "quantity > 0")

    #     self.assertEqual(tx_exp["valid_total_amount"], "total_amount >= 0")
    #     self.assertEqual(order_exp["valid_total_amount"], "total_amount >= 0")

    #     self.assertEqual(tx_exp["valid_product_id"], "product_id IS NOT NULL")
    #     self.assertEqual(order_exp["valid_product_id"], "product_id IS NOT NULL")

    #     self.assertEqual(tx_exp["valid_store_id"], "store_id IS NOT NULL")
    #     self.assertEqual(order_exp["valid_store_id"], "store_id IS NOT NULL")

    def test_config_expectations_aligned(self):
        config_path = Path(__file__).parent.parent / "configs" / "config.yml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        exp = config.get("expectations", {})
        tx_exp = exp.get("transactions", {})
        order_exp = exp.get("orders", {})

        # Flatten the nested category dictionaries
        tx_flat = {k: v for category in tx_exp.values() for k, v in category.items()} if tx_exp else {}
        order_flat = {k: v for category in order_exp.values() for k, v in category.items()} if order_exp else {}

        self.assertIn("valid_order_id", tx_flat)
        self.assertIn("valid_order_id", order_flat)
        self.assertEqual(tx_flat["valid_order_id"], "order_id IS NOT NULL")
        self.assertEqual(order_flat["valid_order_id"], "order_id IS NOT NULL")
        self.assertEqual(tx_flat["valid_quantity"], "quantity > 0")
        self.assertEqual(order_flat["valid_quantity"], "quantity > 0")
        self.assertEqual(tx_flat["valid_total_amount"], "total_amount >= 0")
        self.assertEqual(order_flat["valid_total_amount"], "total_amount >= 0")
        self.assertEqual(tx_flat["valid_product_id"], "product_id IS NOT NULL")
        self.assertEqual(order_flat["valid_product_id"], "product_id IS NOT NULL")
        self.assertEqual(tx_flat["valid_store_id"], "store_id IS NOT NULL")
        self.assertEqual(order_flat["valid_store_id"], "store_id IS NOT NULL")

    def test_deterministic_order_id_generation(self):
        # Simulates: concat(lit("TXN_"), upper(substring(sha2(concat_ws("_", ...), 256), 1, 8)))
        payload = "1997-01-01_1_101_50_3_1997-01-01"
        sha = hashlib.sha256(payload.encode("utf-8")).hexdigest().upper()[:8]
        order_id = f"TXN_{sha}"

        self.assertTrue(order_id.startswith("TXN_"))
        self.assertEqual(len(order_id), 12)  # "TXN_" (4) + 8 hex chars
        # Ensure idempotency
        sha_repeat = hashlib.sha256(payload.encode("utf-8")).hexdigest().upper()[:8]
        self.assertEqual(order_id, f"TXN_{sha_repeat}")

    def test_unified_columns_in_silver_source(self):
        silver_path = Path(__file__).parent.parent / "src" / "maven_market_etl" / "02_silver_transformations.py"
        content = silver_path.read_text(encoding="utf-8")

        expected_cols = [
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

        for col_name in expected_cols:
            self.assertIn(f'"{col_name}"', content, f"Expected {col_name} in UNIFIED_SALES_COLUMNS in 02_silver_transformations.py")

        self.assertIn("transactions_silver", content)
        self.assertIn("orders_silver", content)
        self.assertIn("UNIFIED_SALES_COLUMNS", content)
        self.assertIn("week_of_year", content)
        self.assertIn("data_region", content)

    def test_gold_layer_unification(self):
        gold_path = Path(__file__).parent.parent / "src" / "maven_market_etl" / "03_gold_aggregates.py"
        content = gold_path.read_text(encoding="utf-8")

        self.assertIn("gold_fact_sales", content)
        self.assertIn("tx.unionByName(orders)", content)
        self.assertIn("gold_fact_orders", content)
        self.assertIn('"source_channel"', content)
        self.assertIn('"order_id"', content)
        self.assertIn('"data_region"', content)
        self.assertIn('"week_of_year"', content)
        self.assertIn("yearly_income", content)
        self.assertIn("yearly_income_bracket", content)


if __name__ == "__main__":
    unittest.main()
