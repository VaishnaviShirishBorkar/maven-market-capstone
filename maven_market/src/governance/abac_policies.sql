-- =============================================================================
-- MAVEN MARKET LAKEHOUSE — PII TAGGING, MASKING AND COUNTRY-BASED ACCESS (RBAC)
-- =============================================================================
-- Scope (nothing else):
--   1. Tag PII columns with classification = 'PII'
--   2. Mask every PII-tagged column
--   3. Row-level security by country: USA, Canada, Mexico
--   4. RBAC: one group per country gets read access to the gold tables
--
-- PREREQUISITE: create these 3 ACCOUNT groups and add members before running
-- (Account Console > User management > Groups, or workspace
--  Settings > Identity and access > Groups > Add group):
--     region_usa      -> sees USA stores and customers
--     region_canada   -> sees Canada stores and customers
--     region_mexico   -> sees Mexico stores and customers
-- A user in more than one group sees all of those countries.
-- The data owner (vborkarcapstone@gmail.com) sees all rows and unmasked PII.
-- Everyone else sees no rows and masked PII.
-- =============================================================================

USE CATALOG maven_market;

-- -----------------------------------------------------------------------------
-- 0. ONE-TIME CLEANUP OF THE PREVIOUS SETUP
-- -----------------------------------------------------------------------------
-- Remove tags that are no longer used (only classification = 'PII' is kept)
ALTER SCHEMA maven_market.gold UNSET TAGS ('owner_team', 'data_sensitivity');
ALTER TABLE maven_market.gold.gold_dim_customers  UNSET TAGS ('owner_team', 'data_sensitivity');
ALTER TABLE maven_market.gold.gold_dim_products   UNSET TAGS ('owner_team', 'data_sensitivity');
ALTER TABLE maven_market.gold.gold_dim_stores     UNSET TAGS ('owner_team', 'data_sensitivity', 'data_domain');
ALTER TABLE maven_market.gold.gold_dim_calendar   UNSET TAGS ('owner_team', 'data_sensitivity', 'classification');
ALTER TABLE maven_market.gold.gold_fact_sales     UNSET TAGS ('owner_team', 'data_sensitivity', 'data_domain');
ALTER TABLE maven_market.gold.gold_fact_inventory UNSET TAGS ('owner_team', 'data_sensitivity');
ALTER TABLE maven_market.gold.gold_fact_returns   UNSET TAGS ('owner_team', 'data_sensitivity');
ALTER TABLE maven_market.gold.gold_dim_customers ALTER COLUMN yearly_income UNSET TAGS ('classification');
ALTER TABLE maven_market.gold.gold_fact_sales    ALTER COLUMN gross_profit  UNSET TAGS ('classification');
ALTER TABLE maven_market.gold.gold_fact_sales    ALTER COLUMN total_cost    UNSET TAGS ('classification');
ALTER TABLE maven_market.gold.gold_fact_sales    ALTER COLUMN data_region   UNSET TAGS ('region_filter');

-- Remove the old sales-region row filters (replaced by country filters in section 3)
ALTER MATERIALIZED VIEW maven_market.gold.gold_fact_sales     DROP ROW FILTER;
ALTER MATERIALIZED VIEW maven_market.gold.gold_fact_orders    DROP ROW FILTER;
ALTER MATERIALIZED VIEW maven_market.gold.gold_fact_inventory DROP ROW FILTER;
ALTER MATERIALIZED VIEW maven_market.gold.gold_fact_returns   DROP ROW FILTER;
ALTER MATERIALIZED VIEW maven_market.gold.gold_dim_customers  DROP ROW FILTER;

-- Remove the old blanket grants to `account users` (access now comes from the region groups)
REVOKE USE SCHEMA ON SCHEMA maven_market.gold FROM `account users`;
REVOKE SELECT ON TABLE maven_market.gold.gold_fact_sales     FROM `account users`;
REVOKE SELECT ON TABLE maven_market.gold.gold_fact_orders    FROM `account users`;
REVOKE SELECT ON TABLE maven_market.gold.gold_fact_inventory FROM `account users`;
REVOKE SELECT ON TABLE maven_market.gold.gold_dim_customers  FROM `account users`;
REVOKE SELECT ON TABLE maven_market.gold.gold_dim_stores     FROM `account users`;
REVOKE SELECT ON TABLE maven_market.gold.gold_dim_products   FROM `account users`;
REVOKE SELECT ON TABLE maven_market.gold.gold_dim_calendar   FROM `account users`;
REVOKE SELECT ON TABLE maven_market.audit.user_region_mapping FROM `account users`;
REVOKE EXECUTE ON FUNCTION maven_market.audit.filter_by_sales_region FROM `account users`;
REVOKE EXECUTE ON FUNCTION maven_market.audit.mask_pii_string FROM `account users`;


-- -----------------------------------------------------------------------------
-- 1. PII CLASSIFICATION TAGS
-- -----------------------------------------------------------------------------
-- Silver customer source (streaming table)
ALTER TABLE maven_market.silver.customers_scd ALTER COLUMN first_name           SET TAGS ('classification' = 'PII');
ALTER TABLE maven_market.silver.customers_scd ALTER COLUMN last_name            SET TAGS ('classification' = 'PII');
ALTER TABLE maven_market.silver.customers_scd ALTER COLUMN customer_address     SET TAGS ('classification' = 'PII');
ALTER TABLE maven_market.silver.customers_scd ALTER COLUMN customer_postal_code SET TAGS ('classification' = 'PII');
ALTER TABLE maven_market.silver.customers_scd ALTER COLUMN birthdate            SET TAGS ('classification' = 'PII');

-- Gold customer dimension (materialized view)
ALTER TABLE maven_market.gold.gold_dim_customers ALTER COLUMN full_name  SET TAGS ('classification' = 'PII');
ALTER TABLE maven_market.gold.gold_dim_customers ALTER COLUMN first_name SET TAGS ('classification' = 'PII');
ALTER TABLE maven_market.gold.gold_dim_customers ALTER COLUMN last_name  SET TAGS ('classification' = 'PII');
ALTER TABLE maven_market.gold.gold_dim_customers ALTER COLUMN birthdate  SET TAGS ('classification' = 'PII');


-- -----------------------------------------------------------------------------
-- 2. PII MASKING (applied to every PII-tagged column)
-- -----------------------------------------------------------------------------
-- Text PII: owner sees the real value, everyone else sees first letter + ****
CREATE OR REPLACE FUNCTION maven_market.audit.mask_pii_string(val STRING)
RETURN CASE
  WHEN val IS NULL THEN NULL
  WHEN current_user() = 'vborkarcapstone@gmail.com' THEN val
  WHEN length(val) <= 1 THEN '****'
  ELSE CONCAT(LEFT(val, 1), '****')
END;

-- Date PII (gold birthdate is DATE): owner sees the real value, everyone else sees NULL
CREATE OR REPLACE FUNCTION maven_market.audit.mask_pii_date(val DATE)
RETURN CASE
  WHEN current_user() = 'vborkarcapstone@gmail.com' THEN val
  ELSE NULL
END;

-- Silver: customers_scd is a streaming table (birthdate is STRING here)
ALTER STREAMING TABLE maven_market.silver.customers_scd ALTER COLUMN first_name           SET MASK maven_market.audit.mask_pii_string;
ALTER STREAMING TABLE maven_market.silver.customers_scd ALTER COLUMN last_name            SET MASK maven_market.audit.mask_pii_string;
ALTER STREAMING TABLE maven_market.silver.customers_scd ALTER COLUMN customer_address     SET MASK maven_market.audit.mask_pii_string;
ALTER STREAMING TABLE maven_market.silver.customers_scd ALTER COLUMN customer_postal_code SET MASK maven_market.audit.mask_pii_string;
ALTER STREAMING TABLE maven_market.silver.customers_scd ALTER COLUMN birthdate            SET MASK maven_market.audit.mask_pii_string;

-- Gold: gold_dim_customers is a materialized view
ALTER MATERIALIZED VIEW maven_market.gold.gold_dim_customers ALTER COLUMN full_name  SET MASK maven_market.audit.mask_pii_string;
ALTER MATERIALIZED VIEW maven_market.gold.gold_dim_customers ALTER COLUMN first_name SET MASK maven_market.audit.mask_pii_string;
ALTER MATERIALIZED VIEW maven_market.gold.gold_dim_customers ALTER COLUMN last_name  SET MASK maven_market.audit.mask_pii_string;
ALTER MATERIALIZED VIEW maven_market.gold.gold_dim_customers ALTER COLUMN birthdate  SET MASK maven_market.audit.mask_pii_date;


-- -----------------------------------------------------------------------------
-- 3. ROW-LEVEL SECURITY BY COUNTRY (USA / Canada / Mexico)
-- -----------------------------------------------------------------------------
-- For tables with a country column (customers)
CREATE OR REPLACE FUNCTION maven_market.audit.filter_by_country(p_country STRING)
RETURN current_user() = 'vborkarcapstone@gmail.com'
    OR (p_country = 'USA'    AND is_account_group_member('region_usa'))
    OR (p_country = 'Canada' AND is_account_group_member('region_canada'))
    OR (p_country = 'Mexico' AND is_account_group_member('region_mexico'));

-- For fact tables with store_id: country comes from gold_dim_stores.store_country
CREATE OR REPLACE FUNCTION maven_market.audit.filter_by_store_country(p_store_id INT)
RETURN current_user() = 'vborkarcapstone@gmail.com'
    OR EXISTS (
      SELECT 1
      FROM maven_market.gold.gold_dim_stores s
      WHERE s.store_id = p_store_id
        AND (   (s.store_country = 'USA'    AND is_account_group_member('region_usa'))
             OR (s.store_country = 'Canada' AND is_account_group_member('region_canada'))
             OR (s.store_country = 'Mexico' AND is_account_group_member('region_mexico')))
    );

ALTER MATERIALIZED VIEW maven_market.gold.gold_fact_sales     SET ROW FILTER maven_market.audit.filter_by_store_country ON (store_id);
ALTER MATERIALIZED VIEW maven_market.gold.gold_fact_orders    SET ROW FILTER maven_market.audit.filter_by_store_country ON (store_id);
ALTER MATERIALIZED VIEW maven_market.gold.gold_fact_inventory SET ROW FILTER maven_market.audit.filter_by_store_country ON (store_id);
ALTER MATERIALIZED VIEW maven_market.gold.gold_fact_returns   SET ROW FILTER maven_market.audit.filter_by_store_country ON (store_id);
ALTER MATERIALIZED VIEW maven_market.gold.gold_dim_customers  SET ROW FILTER maven_market.audit.filter_by_country ON (customer_country);


-- -----------------------------------------------------------------------------
-- 4. RBAC: READ ACCESS FOR THE 3 REGION GROUPS
-- -----------------------------------------------------------------------------
-- region_usa
GRANT USE CATALOG ON CATALOG maven_market TO `region_usa`;
GRANT USE SCHEMA ON SCHEMA maven_market.gold TO `region_usa`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_sales     TO `region_usa`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_orders    TO `region_usa`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_inventory TO `region_usa`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_returns   TO `region_usa`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_customers  TO `region_usa`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_stores     TO `region_usa`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_products   TO `region_usa`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_calendar   TO `region_usa`;

-- region_canada
GRANT USE CATALOG ON CATALOG maven_market TO `region_canada`;
GRANT USE SCHEMA ON SCHEMA maven_market.gold TO `region_canada`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_sales     TO `region_canada`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_orders    TO `region_canada`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_inventory TO `region_canada`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_returns   TO `region_canada`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_customers  TO `region_canada`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_stores     TO `region_canada`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_products   TO `region_canada`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_calendar   TO `region_canada`;

-- region_mexico
GRANT USE CATALOG ON CATALOG maven_market TO `region_mexico`;
GRANT USE SCHEMA ON SCHEMA maven_market.gold TO `region_mexico`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_sales     TO `region_mexico`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_orders    TO `region_mexico`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_inventory TO `region_mexico`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_returns   TO `region_mexico`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_customers  TO `region_mexico`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_stores     TO `region_mexico`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_products   TO `region_mexico`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_calendar   TO `region_mexico`;


-- -----------------------------------------------------------------------------
-- 5. VERIFICATION (run as each user to see their view)
-- -----------------------------------------------------------------------------
-- V1: PII-tagged columns
SELECT schema_name, table_name, column_name, tag_value
FROM maven_market.information_schema.column_tags
WHERE tag_name = 'classification'
ORDER BY schema_name, table_name, column_name;

-- V2: Countries visible to the current user (row filter)
SELECT
  current_user() AS evaluated_user,
  s.store_country,
  COUNT(*) AS visible_transactions,
  ROUND(SUM(f.total_amount), 2) AS visible_revenue
FROM maven_market.gold.gold_fact_sales f
JOIN maven_market.gold.gold_dim_stores s ON f.store_id = s.store_id
GROUP BY s.store_country
ORDER BY s.store_country;

-- V3: PII masking and customer country filter
SELECT
  current_user() AS evaluated_user,
  customer_id,
  customer_country,
  full_name,
  first_name,
  last_name,
  birthdate
FROM maven_market.gold.gold_dim_customers
LIMIT 5;

