/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================
Author:         Data Engineering Team
Description:    This stored procedure orchestrates the data ingestion process 
                for the Bronze layer. It performs a full refresh (Truncate & Load)
                of raw data from local CSV files into the SQL Server database.
                
                The Bronze layer is designed to act as a raw data lake area, 
                storing exact copies of source data (CRM and ERP systems) 
                without any transformations.

Execution:      EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	-- ========================================================================
	-- Variable Declaration for Performance Monitoring
	-- ========================================================================
	-- These variables are used to track and log the execution time of 
	-- individual table loads as well as the overall batch process.
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

	-- Begin TRY block to ensure graceful error handling and prevent silent failures.
	BEGIN TRY
		-- Record the start time of the entire data pipeline batch.
		SET @batch_start_time = GETDATE();

		PRINT '==============================================';
		PRINT 'Starting ETL Pipeline: Loading Bronze Layer';
		PRINT '==============================================';

		-- ========================================================================
		-- SOURCE SYSTEM: CRM (Customer Relationship Management)
		-- ========================================================================
		PRINT '==============================================';
		PRINT 'Loading Source: CRM Tables';
		PRINT '==============================================';

		-- ------------------------------------------------------------------------
		-- Table: bronze.crm_cust_info
		-- ------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		-- Step 1: Truncate existing data to perform a full refresh and avoid duplicates.
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		-- Step 2: Bulk load data directly from the flat file.
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\남민우\Desktop\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,           -- Skip the header row
			FIELDTERMINATOR = ',',  -- CSV delimiter
			TABLOCK                 -- Minimal logging and table-level lock for performance
		);
		
		-- [PERFORMANCE NOTE] Validation queries are commented out for production deployment
		-- to eliminate unnecessary network I/O and result-set rendering overhead.
		-- SELECT * FROM bronze.crm_cust_info;
		-- SELECT COUNT(*) FROM bronze.crm_cust_info;
		
		-- Step 3: Calculate and log the execution duration.
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
		PRINT '>> ----------';


		-- ------------------------------------------------------------------------
		-- Table: bronze.crm_prd_info
		-- ------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\남민우\Desktop\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		-- SELECT * FROM bronze.crm_prd_info;
		-- SELECT COUNT(*) FROM bronze.crm_prd_info;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
		PRINT '>> ----------';


		-- ------------------------------------------------------------------------
		-- Table: bronze.crm_sales_details
		-- ------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\남민우\Desktop\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		-- SELECT * FROM bronze.crm_sales_details;
		-- SELECT COUNT(*) FROM bronze.crm_sales_details;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
		PRINT '>> ----------';


		-- ========================================================================
		-- SOURCE SYSTEM: ERP (Enterprise Resource Planning)
		-- ========================================================================
		PRINT '==============================================';
		PRINT 'Loading Source: ERP Tables';
		PRINT '==============================================';

		-- ------------------------------------------------------------------------
		-- Table: bronze.erp_loc_a101
		-- ------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\남민우\Desktop\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		-- SELECT * FROM bronze.erp_loc_a101;
		-- SELECT COUNT(*) FROM bronze.erp_loc_a101;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
		PRINT '>> ----------';


		-- ------------------------------------------------------------------------
		-- Table: bronze.erp_cust_az12
		-- ------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\남민우\Desktop\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		-- SELECT * FROM bronze.erp_cust_az12;
		-- SELECT COUNT(*) FROM bronze.erp_cust_az12;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
		PRINT '>> ----------';


		-- ------------------------------------------------------------------------
		-- Table: bronze.erp_px_cat_g1v2
		-- ------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\남민우\Desktop\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		-- SELECT * FROM bronze.erp_px_cat_g1v2;
		-- SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
		PRINT '>> ----------';

		-- ========================================================================
		-- Pipeline Completion & Audit Logging
		-- ========================================================================
		-- Calculate the total time taken to process all tables.
		SET @batch_end_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Bronze Layer Loading Completed Successfully';
		PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' ms';
		PRINT '==============================================';

	END TRY
	BEGIN CATCH
		-- ========================================================================
		-- Error Handling & Diagnostics
		-- ========================================================================
		-- Catches runtime errors (e.g., file not found, schema mismatch, permission denied)
		-- and outputs system-generated error details to assist in debugging.
		PRINT '==============================================';
		PRINT 'CRITICAL ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: '   + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================';
	END CATCH
END
GO

EXEC bronze.load_bronze;