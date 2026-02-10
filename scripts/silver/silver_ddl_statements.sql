/******************************************************************************************
 Script Name   : Silver Layer Table Creation Script
 Database      : DataWarehouse
 Purpose       : 
    This script prepares the Silver layer of the Data Warehouse by:
    - Dropping existing Silver tables if they already exist
    - Recreating CRM and ERM-related dimension and fact tables
    - Standardizing structure and adding DWH audit columns

    These tables are intended to store cleaned and conformed data
    coming from CRM and ERM source systems.
******************************************************************************************/


PRINT 'Starting Silver layer table creation process...';
PRINT '------------------------------------------------';

/******************************************************************************************
 CRM CUSTOMER INFORMATION TABLE
******************************************************************************************/
PRINT 'Processing table: silver.crm_cust_info';

-- Drop the table if it already exists
IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
BEGIN
    PRINT 'Existing table silver.crm_cust_info found. Dropping table...';
    DROP TABLE silver.crm_cust_info;
END

PRINT 'Creating table silver.crm_cust_info...';

-- Create CRM Customer Information table
CREATE TABLE silver.crm_cust_info (
    cst_ID INT,                                 -- Customer ID
    cst_key NVARCHAR(50),                       -- Business/customer key
    cst_firstname NVARCHAR(50),                 -- Customer first name
    cst_lastname NVARCHAR(50),                  -- Customer last name
    cst_marital_status NVARCHAR(15),            -- Marital status
    cst_gender NVARCHAR(10),                    -- Gender
    cst_create_date DATE,                       -- Customer creation date
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Record load timestamp
);
GO

/******************************************************************************************
 CRM PRODUCT INFORMATION TABLE
******************************************************************************************/
PRINT 'Processing table: silver.crm_prd_info';

-- Drop the table if it already exists
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
BEGIN
    PRINT 'Existing table silver.crm_prd_info found. Dropping table...';
    DROP TABLE silver.crm_prd_info;
END

PRINT 'Creating table silver.crm_prd_info...';

-- Create CRM Product Information table
CREATE TABLE silver.crm_prd_info (
    prd_id INT,                                -- Product ID
    cat_id NVARCHAR(50),                       -- Category ID
    prd_key NVARCHAR(50),                      -- Product business key
    prd_nm NVARCHAR(50),                       -- Product name
    prd_cost INT,                              -- Product cost
    prd_line NVARCHAR(15),                     -- Product line
    prd_start_dt DATE,                         -- Product start date
    prd_end_dt DATE,                           -- Product end date
    dwh_create_date DATETIME2 DEFAULT GETDATE()-- Record load timestamp
);
GO

/******************************************************************************************
 CRM SALES DETAILS FACT TABLE
******************************************************************************************/
PRINT 'Processing table: silver.crm_sales_details';

-- Drop the table if it already exists
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
BEGIN
    PRINT 'Existing table silver.crm_sales_details found. Dropping table...';
    DROP TABLE silver.crm_sales_details;
END

PRINT 'Creating table silver.crm_sales_details...';

-- Create CRM Sales Details fact table
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),                  -- Sales order number
    sls_prd_key NVARCHAR(50),                  -- Product key
    sls_cust_id INT,                           -- Customer ID
    sls_order_dt DATE,                         -- Order date
    sls_ship_dt DATE,                          -- Shipment date
    sls_due_dt DATE,                           -- Due date
    sls_sales INT,                             -- Sales amount
    sls_quantity INT,                          -- Quantity sold
    sls_price INT,                             -- Unit price
    dwh_create_date DATETIME2 DEFAULT GETDATE()-- Record load timestamp
);
GO

/******************************************************************************************
 ERM CUSTOMER AZ12 TABLE
******************************************************************************************/
PRINT 'Processing table: silver.erm_cust_az12';

-- Drop the table if it already exists
IF OBJECT_ID('silver.erm_cust_az12','U') IS NOT NULL
BEGIN
    PRINT 'Existing table silver.erm_cust_az12 found. Dropping table...';
    DROP TABLE silver.erm_cust_az12;
END

PRINT 'Creating table silver.erm_cust_az12...';

-- Create ERM Customer demographic table
CREATE TABLE silver.erm_cust_az12 (
    cid NVARCHAR(50),                          -- Customer ID
    bdate DATE,                                -- Birth date
    gen NVARCHAR(10),                          -- Gender
    dwh_create_date DATETIME2 DEFAULT GETDATE()-- Record load timestamp
);
GO

/******************************************************************************************
 ERM CUSTOMER LOCATION TABLE
******************************************************************************************/
PRINT 'Processing table: silver.erm_loc_a101';

-- Drop the table if it already exists
IF OBJECT_ID('silver.erm_loc_a101','U') IS NOT NULL
BEGIN
    PRINT 'Existing table silver.erm_loc_a101 found. Dropping table...';
    DROP TABLE silver.erm_loc_a101;
END

PRINT 'Creating table silver.erm_loc_a101...';

-- Create ERM Customer location table
CREATE TABLE silver.erm_loc_a101 (
    cid NVARCHAR(50),                          -- Customer ID
    cntry NVARCHAR(50),                        -- Country
    dwh_create_date DATETIME2 DEFAULT GETDATE()-- Record load timestamp
);
GO

/******************************************************************************************
 ERM PRODUCT CATEGORY TABLE
******************************************************************************************/
PRINT 'Processing table: silver.erm_px_cat_g1v2';

-- Drop the table if it already exists
IF OBJECT_ID('silver.erm_px_cat_g1v2','U') IS NOT NULL
BEGIN
    PRINT 'Existing table silver.erm_px_cat_g1v2 found. Dropping table...';
    DROP TABLE silver.erm_px_cat_g1v2;
END

PRINT 'Creating table silver.erm_px_cat_g1v2...';

-- Create ERM Product category and subcategory table
CREATE TABLE silver.erm_px_cat_g1v2 (
    id NVARCHAR(50),                           -- Category ID
    category NVARCHAR(50),                     -- Category name
    subcategory NVARCHAR(50),                  -- Subcategory name
    maintenance NVARCHAR(5),                   -- Maintenance flag
    dwh_create_date DATETIME2 DEFAULT GETDATE()-- Record load timestamp
);
GO

PRINT '------------------------------------------------';
PRINT 'Silver layer table creation process completed successfully.';
