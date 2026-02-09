/* ================================================================================================
Script Purpose:
    This stored procedure orchestrates the "Truncate and Load" process for the Bronze Layer.
    It acts as the entry point for raw data into the Data Warehouse, importing source files 
    from CRM and ERP systems into their respective staging tables.

Key Features:
    - Transactional Logic: Uses TRY...CATCH blocks for robust error handling.
    - Performance: Employs BULK INSERT with TABLOCK for optimized data ingestion.
    - Observability: Tracks and prints the execution duration for each table load.
    - Data Refresh: Performs a full refresh (TRUNCATE) to ensure no stale data remains.
================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    
    BEGIN TRY
        PRINT '-------------------------------------------------------';
        PRINT 'Loading Bronze Layer';
        PRINT '-------------------------------------------------------';

        /* Table: bronze.crm_cust_info
           Action: Full Refresh
           Source: CRM System - Customer Master Data
        */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_cust_info): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        /* Table: bronze.crm_sales_details
           Action: Full Refresh
           Source: CRM System - Transactional Sales Records
        */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_sales_details): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        /* Table: bronze.crm_prd_info
           Action: Full Refresh
           Source: CRM System - Product Catalog Data
        */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_prd_info): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        /* Table: bronze.erm_cust_az12
           Action: Full Refresh
           Source: ERP System (AZ12) - Customer Demographics
        */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erm_cust_az12;
        BULK INSERT bronze.erm_cust_az12
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erm_cust_az12): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        /* Table: bronze.erm_loc_a101
           Action: Full Refresh
           Source: ERP System (A101) - Geographic/Location Mapping
        */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erm_loc_a101;
        BULK INSERT bronze.erm_loc_a101
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erm_loc_a101): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        /* Table: bronze.erm_px_cat_g1v2
           Action: Full Refresh
           Source: ERP System (G1V2) - Product Category Hierarchy
        */
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erm_px_cat_g1v2;
        BULK INSERT bronze.erm_px_cat_g1v2
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erm_px_cat_g1v2): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '-------------------------------------------------------';
        PRINT 'Bronze Layer Loaded Successfully';
        PRINT '-------------------------------------------------------';

    END TRY
    BEGIN CATCH
        /* Error Handling:
           Captures and reports failures during the bulk load process.
        */
        PRINT '=======================================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:   ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=======================================================';
    END CATCH 
END