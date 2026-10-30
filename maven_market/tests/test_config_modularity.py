"""
Tests for config-driven modularity compliance — verifies that config.yml
contains all required expectations, source definitions, and connection
parameters. Also ensures that NO hardcoded values exist in ETL notebooks.

Satisfies Capstone Deliverable:
  - Scoring Category: DevOps & Modularity (15 pts) — 100% config-driven code
  - Scoring Category: Ingestion, Streaming & Logging (15 pts)
"""
import re
import unittest
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None


ROOT = Path(__file__).parent.parent
SRC = ROOT / "src" / "maven_market_etl"


class TestConfigModularity(unittest.TestCase):
    """Ensures config.yml covers all required entities and is complete."""

    def _load_config(self):
        if yaml is None:
            self.skipTest("PyYAML not installed")
        config_path = ROOT / "configs" / "config.yml"
        self.assertTrue(config_path.exists(), "configs/config.yml not found")
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def test_unity_catalog_section_exists(self):
        """Config must define unity_catalog with catalog_name."""
        config = self._load_config()
        uc = config.get("unity_catalog", {})
        self.assertIn("catalog_name", uc, "unity_catalog.catalog_name missing from config.yml")
        self.assertEqual(uc["catalog_name"], "maven_market")

    def test_kafka_section_fully_defined(self):
        """Config must define all Kafka connection parameters — no hardcoded bootstrap servers."""
        config = self._load_config()
        kafka = config.get("kafka", {})
        required = ["bootstrap_servers", "secret_scope", "api_key_name", "api_secret_name",
                    "orders_topic", "inventory_topic"]
        for key in required:
            self.assertIn(key, kafka, f"kafka.{key} missing from config.yml")

    def test_mongodb_section_fully_defined(self):
        """Config must define MongoDB connection parameters — no hardcoded URIs."""
        config = self._load_config()
        mongo = config.get("mongodb", {})
        required = ["database_name", "secret_scope", "secret_key"]
        for key in required:
            self.assertIn(key, mongo, f"mongodb.{key} missing from config.yml")

    def test_pos_csv_sources_defined(self):
        """Config must list all 5 POS CSV source tables."""
        config = self._load_config()
        sources = config.get("pos_csv_sources", [])
        expected_tables = {
            "transactions_raw", "returns_raw", "stores_raw", "regions_raw", "calendar_raw"
        }
        actual_tables = {s["table_name"] for s in sources}
        self.assertEqual(expected_tables, actual_tables,
                         f"Missing POS sources. Expected: {expected_tables}, got: {actual_tables}")

    def test_all_entity_expectations_defined(self):
        """DLT expectations must be defined in config for all 8 data entities."""
        config = self._load_config()
        expectations = config.get("expectations", {})
        required_entities = [
            "transactions", "returns", "orders", "inventory",
            "customers", "products", "stores", "regions", "calendar"
        ]
        for entity in required_entities:
            self.assertIn(entity, expectations, f"DLT expectations missing for entity: {entity}")
            self.assertGreater(
                len(expectations[entity]), 0,
                f"No expectation rules defined for entity: {entity}"
            )

    # def test_transactions_and_orders_have_same_core_expectations(self):
    #     """Transactions and Orders must have identical quality rules to ensure schema unification."""
    #     config = self._load_config()
    #     exp = config.get("expectations", {})
    #     tx = exp.get("transactions", {})
    #     orders = exp.get("orders", {})
    #     shared_keys = ["valid_order_id", "valid_quantity", "valid_product_id",
    #                    "valid_store_id", "valid_total_amount"]
    #     for key in shared_keys:
    #         self.assertIn(key, tx, f"transactions.{key} expectation missing")
    #         self.assertIn(key, orders, f"orders.{key} expectation missing")
    #         self.assertEqual(tx[key], orders[key],
    #                          f"Expectation mismatch between transactions and orders for '{key}'")

    def test_transactions_and_orders_have_same_core_expectations(self):
        """Transactions and Orders must have identical quality rules to ensure schema unification."""
        config = self._load_config()
        exp = config.get("expectations", {})
        
        tx = exp.get("transactions", {})
        orders = exp.get("orders", {})

        # Flatten the nested category dictionaries (drop, warn, fail) for testing
        tx_flat = {k: v for category in tx.values() for k, v in category.items()} if tx else {}
        orders_flat = {k: v for category in orders.values() for k, v in category.items()} if orders else {}

        shared_keys = ["valid_order_id", "valid_quantity", "valid_product_id",
                       "valid_store_id", "valid_total_amount"]
        
        for key in shared_keys:
            self.assertIn(key, tx_flat, f"transactions.{key} expectation missing")
            self.assertIn(key, orders_flat, f"orders.{key} expectation missing")
            self.assertEqual(tx_flat[key], orders_flat[key],
                             f"Expectation mismatch between transactions and orders for '{key}'")


class TestNoHardcodedValues(unittest.TestCase):
    """Ensures ETL notebooks do NOT contain hardcoded credentials, paths, or table names."""

    FORBIDDEN_PATTERNS = [
        # Hardcoded connection strings
        (r"pkc-[a-z0-9]+\..*confluent\.cloud", "Hardcoded Kafka bootstrap server"),
        (r"mongodb\+srv://[^{]", "Hardcoded MongoDB connection URI"),
        (r"abfss://[^\"']*dfs\.core\.windows\.net[^\"']*\"", "Hardcoded ADLS Gen2 path"),
        # Hardcoded credentials
        (r"api_key\s*=\s*['\"][A-Za-z0-9+/]{10,}", "Hardcoded API key"),
        (r"api_secret\s*=\s*['\"][A-Za-z0-9+/]{10,}", "Hardcoded API secret"),
    ]

    def _check_notebook(self, notebook_name):
        nb_path = SRC / notebook_name
        if not nb_path.exists():
            self.skipTest(f"{notebook_name} not found")
        content = nb_path.read_text(encoding="utf-8")
        return content

    def test_bronze_no_hardcoded_credentials(self):
        """01_bronze_ingestion.py must not contain hardcoded credentials or connection strings."""
        content = self._check_notebook("01_bronze_ingestion.py")
        for pattern, description in self.FORBIDDEN_PATTERNS:
            match = re.search(pattern, content)
            if match:
                self.fail(f"Hardcoded value found in bronze notebook: {description} — '{match.group()}'")

    def test_silver_no_hardcoded_credentials(self):
        """02_silver_transformations.py must not contain hardcoded credentials."""
        content = self._check_notebook("02_silver_transformations.py")
        for pattern, description in self.FORBIDDEN_PATTERNS:
            match = re.search(pattern, content)
            if match:
                self.fail(f"Hardcoded value found in silver notebook: {description} — '{match.group()}'")

    def test_gold_no_hardcoded_credentials(self):
        """03_gold_aggregates.py must not contain hardcoded credentials."""
        content = self._check_notebook("03_gold_aggregates.py")
        for pattern, description in self.FORBIDDEN_PATTERNS:
            match = re.search(pattern, content)
            if match:
                self.fail(f"Hardcoded value found in gold notebook: {description} — '{match.group()}'")

    def test_kafka_generator_uses_dbutils_secrets(self):
        """Kafka data generator must retrieve credentials via dbutils.secrets — not hardcoded."""
        gen_path = ROOT / "src" / "data_generators" / "00_kafka_data_generator.py"
        if not gen_path.exists():
            self.skipTest("Kafka generator not found")
        content = gen_path.read_text(encoding="utf-8")
        self.assertIn("dbutils.secrets.get", content,
                      "Kafka generator must use dbutils.secrets.get for credential retrieval")

    def test_config_yml_is_used_in_bronze(self):
        """Bronze notebook must load and use config.yml for all source configurations."""
        content = self._check_notebook("01_bronze_ingestion.py")
        self.assertIn("config.yml", content,
                      "Bronze notebook must load configuration from config.yml")
        self.assertIn("yaml.safe_load", content,
                      "Bronze notebook must parse YAML configuration")


class TestBronzeIngestionCoverage(unittest.TestCase):
    """Validates that bronze ingestion notebook covers all three source types."""

    def _get_bronze_content(self):
        nb_path = SRC / "01_bronze_ingestion.py"
        if not nb_path.exists():
            self.skipTest("01_bronze_ingestion.py not found")
        return nb_path.read_text(encoding="utf-8")

    def test_autoloader_csv_ingestion_present(self):
        """Bronze layer must use Auto Loader (cloudFiles) for CSV ingestion."""
        content = self._get_bronze_content()
        self.assertIn("cloudFiles", content, "Auto Loader (cloudFiles) format not found in bronze notebook")

    def test_mongodb_ingestion_present(self):
        """Bronze layer must connect to MongoDB Atlas for master data ingestion."""
        content = self._get_bronze_content()
        self.assertIn("pymongo", content, "MongoDB (pymongo) connection not found in bronze notebook")

    def test_kafka_streaming_ingestion_present(self):
        """Bronze layer must configure Confluent Kafka streaming ingestion."""
        content = self._get_bronze_content()
        self.assertIn("kafka", content.lower(), "Kafka streaming not found in bronze notebook")
        self.assertIn("readStream", content, "Streaming readStream not found in bronze notebook")

    def test_ingestion_metadata_columns_captured(self):
        """Bronze layer must capture ingestion_timestamp metadata in every table."""
        content = self._get_bronze_content()
        self.assertIn("ingestion_timestamp", content,
                      "ingestion_timestamp metadata column missing from bronze notebook")
        self.assertIn("current_timestamp", content,
                      "current_timestamp() not used for ingestion metadata")

    def test_sasl_jaas_kafka_auth_present(self):
        """Kafka must use SASL_SSL / PLAIN authentication — never plaintext."""
        content = self._get_bronze_content()
        self.assertIn("SASL_SSL", content, "SASL_SSL security protocol not configured in Kafka connection")
        self.assertIn("PLAIN", content, "SASL PLAIN mechanism not configured in Kafka connection")


class TestSilverTransformationCoverage(unittest.TestCase):
    """Validates that silver transformation notebook covers all required transformations."""

    def _get_silver_content(self):
        nb_path = SRC / "02_silver_transformations.py"
        if not nb_path.exists():
            self.skipTest("02_silver_transformations.py not found")
        return nb_path.read_text(encoding="utf-8")

    def test_scd_type2_implemented(self):
        """Silver layer must implement SCD Type 2 for customer and product master data."""
        content = self._get_silver_content()
        self.assertIn("create_auto_cdc_from_snapshot_flow", content,
                      "SCD Type 2 (create_auto_cdc_from_snapshot_flow) not found in silver")
        self.assertIn("stored_as_scd_type=2", content,
                      "stored_as_scd_type=2 not set in SCD configuration")

    def test_schema_unification_constant_defined(self):
        """Silver layer must define UNIFIED_SALES_COLUMNS for schema harmonization."""
        content = self._get_silver_content()
        self.assertIn("UNIFIED_SALES_COLUMNS", content,
                      "UNIFIED_SALES_COLUMNS constant not defined in silver transformations")

    def test_data_region_column_derived(self):
        """Silver layer must derive data_region (NA/EU/APAC) from store_country."""
        content = self._get_silver_content()
        self.assertIn("data_region", content,
                      "data_region column not found in silver transformations")

    def test_dlt_expectations_applied(self):
        """Silver layer must apply DLT data quality expectations from config."""
        content = self._get_silver_content()
        self.assertIn("expect_all_or_drop", content,
                      "DLT expect_all_or_drop quality constraint not found in silver")

    def test_watermark_for_streaming_joins(self):
        """Silver layer must apply watermark for streaming orders table."""
        content = self._get_silver_content()
        self.assertIn("withWatermark", content,
                      "Streaming watermark not found in silver — required for stream-static joins")


class TestGoldLayerGovernanceTags(unittest.TestCase):
    """Validates that governance SQL script enforces structural tags on Gold schema."""

    def _get_governance_content(self):
        sql_path = ROOT / "src" / "governance" / "abac_policies.sql"
        if not sql_path.exists():
            self.skipTest("src/governance/abac_policies.sql not found")
        return sql_path.read_text(encoding="utf-8")

    GOLD_TABLES = [
        "gold_dim_customers", "gold_dim_products", "gold_dim_stores", "gold_dim_calendar",
        "gold_fact_sales", "gold_fact_orders", "gold_fact_inventory", "gold_fact_returns"
    ]

    def test_owner_team_tag_on_all_gold_tables(self):
        """All 8 Gold tables must have owner_team tag set in governance SQL."""
        content = self._get_governance_content()
        for table in self.GOLD_TABLES:
            self.assertIn(table, content,
                          f"Gold table '{table}' not found in governance SQL")
        self.assertIn("owner_team", content,
                      "owner_team structural tag not set in governance SQL")

    def test_data_sensitivity_tag_on_gold(self):
        """All Gold tables must have data_sensitivity tag in governance SQL."""
        content = self._get_governance_content()
        self.assertIn("data_sensitivity", content,
                      "data_sensitivity structural tag not set in governance SQL")

    def test_pii_classification_tag_on_email(self):
        """Customer email column must be tagged classification=PII in governance SQL."""
        content = self._get_governance_content()
        self.assertIn("'classification' = 'PII'", content,
                      "PII classification tag not applied to email column")

    def test_masking_function_created(self):
        """mask_pii_string function must be created for dynamic Column-Level Security."""
        content = self._get_governance_content()
        self.assertIn("mask_pii_string", content,
                      "mask_pii_string CLS function not defined in governance SQL")
        self.assertIn("CREATE OR REPLACE FUNCTION", content,
                      "Masking function not defined with CREATE OR REPLACE FUNCTION")

    def test_row_filter_function_created(self):
        """filter_sales_by_region function must be created for dynamic Row-Level Security."""
        content = self._get_governance_content()
        self.assertIn("filter_sales_by_region", content,
                      "filter_sales_by_region RLS function not defined in governance SQL")

    def test_row_filter_applied_to_fact_tables(self):
        """Row filter must be bound to gold_fact_sales and gold_fact_orders."""
        content = self._get_governance_content()
        self.assertIn("gold_fact_sales", content)
        self.assertIn("gold_fact_orders", content)
        self.assertIn("SET ROW FILTER", content,
                      "SET ROW FILTER not found — row filter not bound to fact tables")

    def test_column_mask_applied_to_customer_email(self):
        """Column mask must be bound to gold_dim_customers.email."""
        content = self._get_governance_content()
        self.assertIn("SET MASK", content,
                      "SET MASK not found — column masking not applied to email columns")
        self.assertIn("gold_dim_customers", content,
                      "Column mask not applied to gold_dim_customers")

    def test_user_region_mapping_referenced(self):
        """RLS function must reference audit.user_region_mapping for dynamic territory lookup."""
        content = self._get_governance_content()
        self.assertIn("user_region_mapping", content,
                      "audit.user_region_mapping not referenced in RLS filter function")

    def test_gold_governance_does_not_alter_bronze_or_silver_tags(self):
        """Structural owner_team and data_sensitivity tags must ONLY be applied to Gold — not Bronze/Silver."""
        content = self._get_governance_content()

        # Parse each SET TAGS statement to check it targets only gold tables
        set_tags_blocks = re.findall(
            r"ALTER TABLE maven_market\.(bronze|silver)\..*?SET TAGS[^;]*owner_team[^;]*;",
            content, re.DOTALL | re.IGNORECASE
        )
        self.assertEqual(
            len(set_tags_blocks), 0,
            f"owner_team structural tag must only be on Gold, but found it applied to Bronze/Silver: {set_tags_blocks}"
        )


if __name__ == "__main__":
    unittest.main()
