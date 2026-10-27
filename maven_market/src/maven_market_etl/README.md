# maven_market

This folder defines all source code for the maven_market pipeline:

- `explorations/`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations/`: All dataset definitions and transformations.
- `utilities/` (optional): Utility functions and Python modules used in this pipeline.
- `data_sources/` (optional): View definitions describing the source data for this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* Take a look at the sample called "sample_trips_maven_market.py" to get familiar with the syntax.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run maven_market_etl --refresh sample_trips_maven_market` to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.

## Gold layer and dashboard tables

`03_gold_aggregates.py` defines eight Gold outputs:

- Dimensions: `gold_dim_customers`, `gold_dim_products`, `gold_dim_stores`, `gold_dim_calendar`
- Facts: `gold_fact_sales`, `gold_fact_returns`, `gold_fact_orders`, `gold_fact_inventory`

Dashboard mapping:

- Executive Overview: `gold_fact_sales` with calendar/product/store dimensions.
- Regional Sales: `gold_fact_sales` with `gold_dim_stores`.
- Real-Time Operations: `gold_fact_orders` and `gold_fact_inventory`.
- Platform Health: the `audit.audit_logs` table and Databricks pipeline event/expectation metrics; this is operational telemetry, not a business fact table.

### Schema Unification & Gold Combination

`transactions_raw` (pre-given historical POS CSV data) and `orders_raw` (real-time Kafka event data) are standardized in the Silver layer into a shared, unified schema across `transactions_silver` and `orders_silver`:

- **Order Identification**: A deterministic, content-hashed `order_id` (`TXN_<hash>`) is generated for POS transactions to mirror the Kafka `order_id` (`ORD_<hash>`).
- **Temporal Alignment**: Standardized `order_timestamp`, `order_date`, `transaction_date`, and `stock_date` columns are provided in both tables.
- **Dimension Enrichment**: Both streams join against `products_scd` and `stores_silver` to append dimension keys, store names, product branding, retail prices, and costs.
- **Financial Metrics**: Both tables calculate `total_amount`, `calculated_amount`, `total_cost`, `gross_profit`, and `profit_margin_pct`.
- **Channel Lineage**: A `source_channel` column (`POS` vs `KAFKA`) identifies origin while preserving schema parity.

In the Gold layer:
- `gold_fact_sales` combines `transactions_silver` and `orders_silver` via `unionByName` into a single enterprise Kimball fact table for Executive Overview and Regional Sales dashboards.
- `gold_fact_orders` reads `orders_silver` via `spark.readStream.table(...)` to provide low-latency incremental data for Real-Time Operations.
- `gold_fact_inventory` preserves each observation; for a current-stock KPI, select the latest row per product/store rather than summing historical stock observations.

The source code has been syntax-checked, but it has not been executed against a live Databricks workspace. Validate pipeline API/runtime compatibility, source table schemas, streaming checkpoints, and end-to-end dashboard queries after deployment.
