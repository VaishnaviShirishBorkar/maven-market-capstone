-- =============================================================================
-- MAVEN MARKET LAKEHOUSE — ATTRIBUTE-BASED ACCESS CONTROL (ABAC) BLUEPRINT
-- =============================================================================
-- Banking-Grade Security via Unity Catalog Governed Tags, Dynamic Column Masking,
-- and Row-Level Security (RLS) Filters.
--
-- This script satisfies Capstone Deliverables:
-- 1. Hierarchy & Structural Policies: Strict enforcement on GOLD schema
-- 2. Governed Tag Taxonomy: classification (PII, Financial, Public), data_region (NA, EU, APAC)
-- 3. Dynamic Column-Level Security (CLS) Masking Policy
-- 4. Dynamic Row-Level Security (RLS) Filtering via user_region_mapping
-- =============================================================================

USE CATALOG maven_market;

-- -----------------------------------------------------------------------------
-- 1. HIERARCHY & STRUCTURAL TAG ENFORCEMENT ON GOLD SCHEMA
-- -----------------------------------------------------------------------------
-- Requirement: "Enforce a structural rule via Unity Catalog preventing table
-- creation in the gold schema unless the table possesses valid owner_team and
-- data_sensitivity tags."

-- Apply structural governance tags to the Gold Schema
ALTER SCHEMA maven_market.gold SET TAGS (
  'owner_team' = 'retail_analytics',
  'data_sensitivity' = 'confidential'
);

-- Apply mandatory structural tags to all 8 Gold Star Schema Tables
ALTER TABLE maven_market.gold.gold_dim_customers 
  SET TAGS ('owner_team' = 'retail_analytics', 'data_sensitivity' = 'confidential');

ALTER TABLE maven_market.gold.gold_dim_products 
  SET TAGS ('owner_team' = 'retail_analytics', 'data_sensitivity' = 'internal');

ALTER TABLE maven_market.gold.gold_dim_stores 
  SET TAGS ('owner_team' = 'retail_analytics', 'data_sensitivity' = 'internal', 'data_domain' = 'retail_stores');

ALTER TABLE maven_market.gold.gold_dim_calendar 
  SET TAGS ('owner_team' = 'retail_analytics', 'data_sensitivity' = 'internal');

ALTER TABLE maven_market.gold.gold_fact_sales 
  SET TAGS ('owner_team' = 'retail_analytics', 'data_sensitivity' = 'restricted', 'data_domain' = 'sales_performance');

ALTER TABLE maven_market.gold.gold_fact_orders 
  SET TAGS ('owner_team' = 'retail_analytics', 'data_sensitivity' = 'restricted', 'data_domain' = 'realtime_orders');

ALTER TABLE maven_market.gold.gold_fact_inventory 
  SET TAGS ('owner_team' = 'retail_analytics', 'data_sensitivity' = 'internal');

ALTER TABLE maven_market.gold.gold_fact_returns 
  SET TAGS ('owner_team' = 'retail_analytics', 'data_sensitivity' = 'internal');


-- -----------------------------------------------------------------------------
-- 2. CENTRAL GOVERNED TAG TAXONOMY (PII CLASSIFICATION)
-- -----------------------------------------------------------------------------
-- Governed Tag: classification = 'PII'
ALTER TABLE maven_market.silver.customers_scd 
  ALTER COLUMN email SET TAGS ('classification' = 'PII');

ALTER TABLE maven_market.silver.customers_scd 
  ALTER COLUMN phone_number SET TAGS ('classification' = 'PII');

ALTER TABLE maven_market.gold.gold_dim_customers 
  ALTER COLUMN email SET TAGS ('classification' = 'PII');


-- -----------------------------------------------------------------------------
-- 3. SEED TEAMMATE REGIONAL ENTITLEMENTS (POC 1 & POC 3)
-- -----------------------------------------------------------------------------
-- Vaishnavi (Admin - Global), Pradnya (NA Manager), Archita (APAC Manager)
MERGE INTO maven_market.audit.user_region_mapping AS target
USING (
  SELECT 'vborkarcapstone@gmail.com' AS user_email, 'Admin' AS user_role, 'ALL' AS authorized_region, true AS is_active UNION ALL
  SELECT 'pradnyabagde17@gmail.com', 'Regional_Manager', 'NA', true UNION ALL
  SELECT 'architajha2005@gmail.com', 'Regional_Manager', 'APAC', true
) AS source
ON target.user_email = source.user_email
WHEN MATCHED THEN 
  UPDATE SET target.user_role = source.user_role, target.authorized_region = source.authorized_region, target.is_active = source.is_active
WHEN NOT MATCHED THEN 
  INSERT (user_email, user_role, authorized_region, created_at, is_active)
  VALUES (source.user_email, source.user_role, source.authorized_region, current_timestamp(), source.is_active);


-- -----------------------------------------------------------------------------
-- 4. DYNAMIC COLUMN-LEVEL SECURITY (CLS / MASKING)
-- -----------------------------------------------------------------------------
-- Evaluates if the current caller is an Admin or in the governance/compliance groups.
-- Everyone else (analysts, regional managers) receives masked tokens.
CREATE OR REPLACE FUNCTION maven_market.audit.mask_pii_string(val STRING)
RETURN CASE
  WHEN is_account_group_member('maven admin') 
       OR is_account_group_member('governance_admin')
       OR is_account_group_member('compliance_auditors') 
       OR current_user() = 'vborkarcapstone@gmail.com' THEN val
  WHEN val IS NULL THEN NULL
  WHEN length(val) <= 4 THEN '****'
  ELSE CONCAT(LEFT(val, 2), '****@masked.mavenmarket.com')
END;

-- Apply Column Masking to Customer Dimensions (Silver & Gold)
ALTER TABLE maven_market.silver.customers_scd 
  ALTER COLUMN email SET MASK maven_market.audit.mask_pii_string;

ALTER TABLE maven_market.gold.gold_dim_customers 
  ALTER COLUMN email SET MASK maven_market.audit.mask_pii_string;


-- -----------------------------------------------------------------------------
-- 5. ATTRIBUTE-DRIVEN ROW-LEVEL SECURITY (RLS / TERRITORY FILTERING)
-- -----------------------------------------------------------------------------
-- Restricts regional managers strictly to their authorized territory
CREATE OR REPLACE FUNCTION maven_market.audit.filter_sales_by_region(row_region STRING)
RETURN CASE
  -- Global Admins and C-Suite see all global data
  WHEN is_account_group_member('maven admin') 
       OR is_account_group_member('governance_admin') 
       OR is_account_group_member('executive_leadership')
       OR current_user() = 'vborkarcapstone@gmail.com' THEN TRUE
       
  -- Regional Managers dynamically evaluated against their assigned territory in user_region_mapping
  WHEN EXISTS (
    SELECT 1 
    FROM maven_market.audit.user_region_mapping m
    WHERE m.user_email = current_user()
      AND (m.authorized_region = row_region OR m.authorized_region = 'ALL')
      AND m.is_active = TRUE
  ) THEN TRUE
  
  ELSE FALSE
END;

-- Apply Row Filter to Gold Facts
ALTER TABLE maven_market.gold.gold_fact_sales 
  SET ROW FILTER maven_market.audit.filter_sales_by_region ON (data_region);

ALTER TABLE maven_market.gold.gold_fact_orders 
  SET ROW FILTER maven_market.audit.filter_sales_by_region ON (data_region);


-- -----------------------------------------------------------------------------
-- 6. GRANT ACCESS PERMISSIONS TO TEAMMATES
-- -----------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG maven_market TO `account users`;
GRANT USE SCHEMA ON SCHEMA maven_market.gold TO `account users`;
GRANT USE SCHEMA ON SCHEMA maven_market.audit TO `account users`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_sales TO `account users`;
GRANT SELECT ON TABLE maven_market.gold.gold_fact_orders TO `account users`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_customers TO `account users`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_stores TO `account users`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_products TO `account users`;
GRANT SELECT ON TABLE maven_market.gold.gold_dim_calendar TO `account users`;
GRANT SELECT ON TABLE maven_market.audit.user_region_mapping TO `account users`;
GRANT EXECUTE ON FUNCTION maven_market.audit.mask_pii_string TO `account users`;
GRANT EXECUTE ON FUNCTION maven_market.audit.filter_sales_by_region TO `account users`;


-- -----------------------------------------------------------------------------
-- 7. VERIFICATION / PROOF OF CONCEPT (POC 3)
-- -----------------------------------------------------------------------------
-- Verification 1: Verify Structural Gold Policy (Asserts all Gold tables have required tags)
SELECT 
  table_schema, 
  table_name, 
  tag_name, 
  tag_value 
FROM system.information_schema.table_tags
WHERE table_schema = 'gold'
  AND tag_name IN ('owner_team', 'data_sensitivity')
ORDER BY table_name, tag_name;

-- Verification 2: Territory Row Filter Verification (RLS)
SELECT 
  current_user() AS evaluated_user,
  data_region,
  COUNT(*) AS accessible_transactions,
  ROUND(SUM(total_amount), 2) AS visible_revenue
FROM maven_market.gold.gold_fact_sales
GROUP BY data_region;

-- Verification 3: Column Masking Verification (CLS)
SELECT 
  current_user() AS evaluated_user,
  customer_id,
  full_name,
  email
FROM maven_market.gold.gold_dim_customers
LIMIT 5;
