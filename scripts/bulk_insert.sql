USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE  bronze.load_bronze AS 
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME
    BEGIN TRY

        /* ============================================================
           Load: bronze.crm_cust_info
           Purpose:
             Reloads raw customer master data from CRM source files.
           Layer:
             Bronze (raw ingestion).
           Load Strategy:
             - Full refresh (TRUNCATE + BULK INSERT).
             - Header row skipped.
           ============================================================ */
        SET @start_time=GETDATE()
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,             -- Skip header row in CSV
            FIELDTERMINATOR = ',',    -- Comma-separated values
            TABLOCK                   -- Improves bulk load performance
        )
        SET @end_time=GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(seconds,@start_time,@end_time) AS NVARCHAR) + ' seconds'
        /* ============================================================
           Load: bronze.crm_sales_details
           Purpose:
             Loads raw transactional sales data from CRM system.
           Layer:
             Bronze.
           Load Strategy:
             - Full reload of sales data.
             - Dates and measures ingested as received from source.
           ============================================================ */
        SET @start_time=GETDATE()
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time=GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(seconds,@start_time,@end_time) AS NVARCHAR) + ' seconds'

        /* ============================================================
           Load: bronze.crm_prd_info
           Purpose:
             Loads raw product master data from CRM source.
           Layer:
             Bronze.
           Load Strategy:
             - Full refresh to capture complete product catalog.
           ============================================================ */
        SET @start_time=GETDATE()
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time=GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(seconds,@start_time,@end_time) AS NVARCHAR) + ' seconds'

        /* ============================================================
           Load: bronze.erm_cust_az12
           Purpose:
             Loads customer demographic attributes from ERM source AZ12.
           Layer:
             Bronze.
           Load Strategy:
             - Full reload for downstream customer enrichment.
           ============================================================ */
        SET @start_time=GETDATE()
        TRUNCATE TABLE bronze.erm_cust_az12;

        BULK INSERT bronze.erm_cust_az12
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time=GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(seconds,@start_time,@end_time) AS NVARCHAR) + ' seconds'

        /* ============================================================
           Load: bronze.erm_loc_a101
           Purpose:
             Loads customer location data from ERM source A101.
           Layer:
             Bronze.
           Load Strategy:
             - Full refresh of customer-country mapping.
           ============================================================ */
        SET @start_time=GETDATE()
        TRUNCATE TABLE bronze.erm_loc_a101;

        BULK INSERT bronze.erm_loc_a101
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time=GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(seconds,@start_time,@end_time) AS NVARCHAR) + ' seconds'

        /* ============================================================
           Load: bronze.erm_px_cat_g1v2
           Purpose:
             Loads product category and subcategory reference data
             from ERM source.
           Layer:
             Bronze.
           Load Strategy:
             - Full reload of category hierarchy metadata.
           ============================================================ */
        SET @start_time=GETDATE()
        TRUNCATE TABLE bronze.erm_px_cat_g1v2;

        BULK INSERT bronze.erm_px_cat_g1v2
        FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time=GETDATE()
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(seconds,@start_time,@end_time) AS NVARCHAR) + ' seconds'

    END TRY
    BEGIN CATCH
    PRINT 'Error occured during loading Bronze Layer'
    PRINT 'Error Message' + ERROR_MESSAGE();
    PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR)
    END CATCH 
END
