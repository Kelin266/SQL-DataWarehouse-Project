USE DataWarehouse;
GO

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
TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
    FIRSTROW = 2,             -- Skip header row in CSV
    FIELDTERMINATOR = ',',    -- Comma-separated values
    TABLOCK                   -- Improves bulk load performance
);
GO

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
TRUNCATE TABLE bronze.crm_sales_details;

BULK INSERT bronze.crm_sales_details
FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

/* ============================================================
   Load: bronze.crm_prd_info
   Purpose:
     Loads raw product master data from CRM source.
   Layer:
     Bronze.
   Load Strategy:
     - Full refresh to capture complete product catalog.
   ============================================================ */
TRUNCATE TABLE bronze.crm_prd_info;

BULK INSERT bronze.crm_prd_info
FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

/* ============================================================
   Load: bronze.erm_cust_az12
   Purpose:
     Loads customer demographic attributes from ERM source AZ12.
   Layer:
     Bronze.
   Load Strategy:
     - Full reload for downstream customer enrichment.
   ============================================================ */
TRUNCATE TABLE bronze.erm_cust_az12;

BULK INSERT bronze.erm_cust_az12
FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

/* ============================================================
   Load: bronze.erm_loc_a101
   Purpose:
     Loads customer location data from ERM source A101.
   Layer:
     Bronze.
   Load Strategy:
     - Full refresh of customer-country mapping.
   ============================================================ */
TRUNCATE TABLE bronze.erm_loc_a101;

BULK INSERT bronze.erm_loc_a101
FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

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
TRUNCATE TABLE bronze.erm_px_cat_g1v2;

BULK INSERT bronze.erm_px_cat_g1v2
FROM 'D:\SQL\Projects\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO
