/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script defines the Data Definition Language (DDL) for the 'silver' schema.
    It creates cleansed and standardized tables for both CRM and ERP sources.
    
    Idempotent Design: 
    Drops existing tables if they exist before recreation to ensure the script 
    can be run safely multiple times.

    Auditability:
    Includes a 'dwh_create_date' column (DATETIME2) in all tables to track 
    when the record was ingested into the data warehouse.
===============================================================================
*/

-- ============================================================================
-- CRM Source Tables
-- ============================================================================

/*
-------------------------------------------------------------------------------
Table: silver.crm_cust_info
Description: Cleansed customer demographic information from the CRM system.
-------------------------------------------------------------------------------
*/
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

/*
-------------------------------------------------------------------------------
Table: silver.crm_prd_info
Description: Cleansed product catalog and pricing details from the CRM system.
Note: 'cat_id' is a derived column extracted from 'prd_key' during ETL.
-------------------------------------------------------------------------------
*/
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id             INT,
    cat_id             NVARCHAR(50), -- Derived column from transformation
    prd_key            NVARCHAR(50),
    prd_nm             NVARCHAR(50),
    prd_cost           INT,
    prd_line           NVARCHAR(50),
    prd_start_dt       DATE,         -- Standardized to DATE type
    prd_end_dt         DATE,         -- Standardized to DATE type
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

/*
-------------------------------------------------------------------------------
Table: silver.crm_sales_details
Description: Cleansed transactional sales records from the CRM system.
Note: Date columns are explicitly cast to DATE to resolve type mismatches.
-------------------------------------------------------------------------------
*/
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num        NVARCHAR(50),
    sls_prd_key        NVARCHAR(50),
    sls_cust_id        INT,
    sls_order_dt       DATE,         -- Standardized to DATE type
    sls_ship_dt        DATE,         -- Standardized to DATE type
    sls_due_dt         DATE,         -- Standardized to DATE type
    sls_sales          INT,
    sls_quantity       INT,
    sls_price          INT,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- ERP Source Tables
-- ============================================================================

/*
-------------------------------------------------------------------------------
Table: silver.erp_loc_a101
Description: Cleansed location mapping data from the ERP system.
-------------------------------------------------------------------------------
*/
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid                NVARCHAR(50),
    cntry              NVARCHAR(50),
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

/*
-------------------------------------------------------------------------------
Table: silver.erp_cust_az12
Description: Cleansed customer personal data from the ERP system.
-------------------------------------------------------------------------------
*/
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid                NVARCHAR(50),
    bdate              DATE,
    gen                NVARCHAR(50),
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

/*
-------------------------------------------------------------------------------
Table: silver.erp_px_cat_g1v2
Description: Cleansed product category and maintenance hierarchy from the ERP.
-------------------------------------------------------------------------------
*/
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id                 NVARCHAR(50),
    cat                NVARCHAR(50),
    subcat             NVARCHAR(50),
    maintenance        NVARCHAR(50),
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO