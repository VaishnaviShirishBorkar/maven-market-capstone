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

`gold_fact_orders` and `gold_fact_inventory` read their Silver sources with `spark.readStream.table(...)` to keep the Gold outputs incremental. `gold_fact_inventory` preserves each observation; for a current-stock KPI, select the latest row per product/store rather than summing historical stock observations. `gold_fact_sales` calculates gross profit from the product retail price and cost directly, instead of referencing a column that is not present in `products_scd`.

The source code has been syntax-checked, but it has not been executed against a live Databricks workspace. Validate pipeline API/runtime compatibility, source table schemas, streaming checkpoints, and end-to-end dashboard queries after deployment.
