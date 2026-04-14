/*
===============================================================================
Quality Assurance (QA) Script: Silver Layer Data Validation
===============================================================================
Script Purpose:
    This script performs comprehensive quality checks across all 'silver' layer 
    tables. It is designed to ensure data integrity, consistency, and 
    standardization before the data is promoted to the Gold layer (Data Mart).

    Validation Rules Applied:
    - Primary Key Integrity: Checks for NULLs or duplicate keys.
    - Data Cleansing: Identifies unwanted trailing/leading spaces.
    - Business Logic: Validates date ranges and chronological order.
    - Mathematical Consistency: Verifies calculated fields (e.g., Sales = Qty * Price).
    - Categorical Standardization: Reviews distinct values for dimension attributes.

Usage Notes:
    - Execute this script after running the 'silver.load_silver' procedure.
    - Any queries returning results indicate a Data Quality (DQ) violation that 
      requires investigation in the ETL transformation logic.
===============================================================================
*/

-- ============================================================================
-- 1. Checking 'silver.crm_cust_info'
-- ============================================================================

-- Rule: Primary keys must be unique and not null.
-- Expectation: 0 rows returned.
SELECT 
    cst_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Rule: String fields should not contain leading or trailing whitespaces.
-- Expectation: 0 rows returned.
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Rule: Categorical values must be standardized.
-- Action: Manually review the output for unexpected values (e.g., typos, lowercases).
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;


-- ============================================================================
-- 2. Checking 'silver.crm_prd_info'
-- ============================================================================

-- Rule: Primary keys must be unique and not null.
-- Expectation: 0 rows returned.
SELECT 
    prd_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Rule: Product names should be properly trimmed.
-- Expectation: 0 rows returned.
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Rule: Product cost cannot be negative or missing.
-- Expectation: 0 rows returned.
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Rule: Product lines must map to standard categories.
-- Action: Manually review the distinct values.
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Rule: Chronological validity (End Date must be after Start Date).
-- Expectation: 0 rows returned.
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ============================================================================
-- 3. Checking 'silver.crm_sales_details'
-- ============================================================================

-- [FIXED] Rule: Due dates must fall within a reasonable business timeframe.
-- Note: Evaluated against the DATE data type used in the Silver layer.
-- Expectation: 0 rows returned (No out-of-bounds dates).
SELECT 
    sls_due_dt 
FROM silver.crm_sales_details
WHERE sls_due_dt > '2050-01-01' 
   OR sls_due_dt < '1900-01-01';

-- Rule: Chronological validity (Order must precede Shipping and Due dates).
-- Expectation: 0 rows returned.
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Rule: Mathematical consistency across transaction metrics.
-- Formula: Total Sales = Quantity * Price
-- Expectation: 0 rows returned.
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- ============================================================================
-- 4. Checking 'silver.erp_cust_az12'
-- ============================================================================

-- Rule: Birthdates must be logically possible (Not in the future, not too old).
-- Expectation: 0 rows returned.
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Rule: Gender values must be standardized.
-- Action: Manually review the distinct values ('Male', 'Female', 'n/a').
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;


-- ============================================================================
-- 5. Checking 'silver.erp_loc_a101'
-- ============================================================================

-- Rule: Country names must be fully spelled out and standardized.
-- Action: Manually review for anomalies (e.g., 'DE', 'USA' instead of 'Germany').
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;


-- ============================================================================
-- 6. Checking 'silver.erp_px_cat_g1v2'
-- ============================================================================

-- Rule: Hierarchical string fields should not contain unwanted spaces.
-- Expectation: 0 rows returned.
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Rule: Maintenance categories must be consistent.
-- Action: Manually review distinct values.
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;