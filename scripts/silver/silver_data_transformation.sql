/* ================================================================================================
Script Purpose:
    This script performs the ETL process for the Silver Layer. It cleanses, standardizes, 
    and validates raw data from the Bronze schema before inserting it into the Silver tables.
    
Key Transformations:
    - Deduplication using Window Functions (ROW_NUMBER).
    - Data Standardisation (Gender, Marital Status, Country Names).
    - Type Casting and Date Formatting.
    - Derived Column Logic (Product End Dates, Sales recalculations).
    - Handling NULLs and invalid business keys.
================================================================================================
*/


-- Creating a stored procedure for schema 'silver'
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT '===============================================================';
		PRINT 'LOADING SILVER LAYER';
		PRINT '===============================================================';

		PRINT '---------------------------------------------------------------';
		PRINT 'LOAD CRM TABLES';
		PRINT '---------------------------------------------------------------';

		-- 1. Transform and Load: silver.crm_cust_info
		-- Purpose: Deduplicates customers and standardizes personal attributes.
		SET @start_time=GETDATE();
		PRINT '>> TRUNCATING TABLE : silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> INSERTING INTO TABLE : silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
				 ELSE 'N/A'
			END AS cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gender))='F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gender))='M' THEN 'Male'
				 ELSE 'N/A'
			END AS cst_gender,
			cst_create_date
		FROM (
			SELECT 
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
		) t
		WHERE flag_last = 1 AND cst_id IS NOT NULL;

		SET @end_time=GETDATE()
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		-- 2. Transform and Load: silver.crm_prd_info
		-- Purpose: Splits composite keys, handles product lines, and calculates SCD-like end dates.
		SET @start_time=GETDATE();
		PRINT '>> TRUNCATING TABLE : silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> INSERTING INTO TABLE : silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)), '-', '_') AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt
		FROM bronze.crm_prd_info;

		SET @end_time=GETDATE()
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'


		-- 3. Transform and Load: silver.crm_sales_details
		-- Purpose: Converts integer dates to DATE types and enforces data integrity on pricing and sales.
		SET @start_time=GETDATE();
		PRINT '>> TRUNCATING TABLE : silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> INSERTING INTO TABLE : silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_quantity,
			sls_price,
			sls_sales
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE WHEN LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
			CASE WHEN LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
			sls_quantity,
			CASE WHEN sls_price < 0 THEN ABS(sls_price)
				 WHEN sls_price IS NULL THEN sls_sales / sls_quantity
				 ELSE sls_price
			END sls_price,
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * sls_price 
				 THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END sls_sales
		FROM bronze.crm_sales_details;

		SET @end_time=GETDATE()
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		PRINT '---------------------------------------------------------------';
		PRINT 'LOAD ERP TABLES';
		PRINT '---------------------------------------------------------------';

		-- 4. Transform and Load: silver.erm_cust_az12
		-- Purpose: Cleanses external ERM customer data, removing prefixes and validating birthdates.
		SET @start_time=GETDATE()
		PRINT '>> TRUNCATING TABLE : silver.erm_cust_az12'
		TRUNCATE TABLE silver.erm_cust_az12;
		PRINT '>> INSERTING INTO TABLE : silver.erm_cust_az12'
		INSERT INTO silver.erm_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				 ELSE cid
			END cid,
			CASE WHEN bdate > '2024-01-01' THEN NULL
				 ELSE bdate
			END bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				 ELSE 'N/A'
			END gen
		FROM bronze.erm_cust_az12;

		SET @end_time=GETDATE()
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		-- 5. Transform and Load: silver.erm_loc_a101
		-- Purpose: Standardizes country codes into full names and cleanses customer IDs.
		SET @start_time=GETDATE()
		PRINT '>> TRUNCATING TABLE : silver.erm_loc_a101'
		TRUNCATE TABLE silver.erm_loc_a101;
		PRINT '>> INSERTING INTO TABLE : silver.erm_loc_a101'
		INSERT INTO silver.erm_loc_a101(
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') cid,
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United Sates'
				 WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'N/A'
				 ELSE cntry
			END cntry
		FROM bronze.erm_loc_a101;

		SET @end_time=GETDATE()
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		-- 6. Transform and Load: silver.erm_px_cat_g1v2
		-- Purpose: Simple ingestion of product category and subcategory mapping.
		SET @start_time=GETDATE()
		PRINT '>> TRUNCATING TABLE : silver.erm_px_cat_g1v2'
		TRUNCATE TABLE silver.erm_px_cat_g1v2;
		PRINT '>> INSERTING INTO TABLE : silver.erm_px_cat_g1v2'
		INSERT INTO silver.erm_px_cat_g1v2(
			id,
			category,
			subcategory,
			maintenance
		)
		SELECT
			id,
			category,
			subcategory,
			maintenance
		FROM bronze.erm_px_cat_g1v2;

		SET @end_time=GETDATE()
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		
		SET @batch_end_time=GETDATE()
		PRINT '===============================================================';
		PRINT 'LOADING SILVER LAYER COMPLETED.';
		PRINT 'TOTAL BATCH DURATION: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '===============================================================';

	END TRY
	BEGIN CATCH
		PRINT '===============================================================';
		PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===============================================================';
	END CATCH
END