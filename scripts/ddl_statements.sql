USE DataWarehouse;
GO

/* ============================================================
   Table: bronze.crm_cust_info
   Purpose:
     Stores raw customer master data extracted from the CRM system.
   Layer:
     Bronze (raw ingestion, minimal transformation).
   Notes:
     - One record per customer.
     - Data is ingested as-is from source files.
   ============================================================ */
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_ID INT,                       -- Internal customer identifier
    cst_key NVARCHAR(50),             -- Business/customer key from source system
    cst_firstname NVARCHAR(50),       -- Customer first name
    cst_lastname NVARCHAR(50),        -- Customer last name
    cst_marital_status NVARCHAR(15),  -- Marital status (raw CRM value)
    cst_gender NVARCHAR(10),          -- Gender as received from CRM
    cst_create_date DATE              -- Customer creation date in CRM
);
GO

/* ============================================================
   Table: bronze.crm_prd_info
   Purpose:
     Stores raw product master data from the CRM system.
   Layer:
     Bronze.
   Notes:
     - Contains product lifecycle dates.
     - No business rules applied at this stage.
   ============================================================ */
IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,                       -- Internal product identifier
    prd_key NVARCHAR(50),             -- Product business key
    prd_nm NVARCHAR(50),              -- Product name
    prd_cost INT,                     -- Product cost (raw value)
    prd_line NVARCHAR(5),             -- Product line/category code
    prd_start_dt DATETIME,            -- Product availability start date
    prd_end_dt DATETIME               -- Product availability end date
);
GO

/* ============================================================
   Table: bronze.crm_sales_details
   Purpose:
     Stores raw transactional sales data from CRM.
   Layer:
     Bronze.
   Notes:
     - Date fields are stored as integers (as received from source).
     - This table is expected to be large and append-only.
   ============================================================ */
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),         -- Sales order number
    sls_prd_key NVARCHAR(50),         -- Product key sold
    sls_cust_id INT,                  -- Customer identifier
    sls_order_dt INT,                 -- Order date (raw integer format)
    sls_ship_dt INT,                  -- Shipping date (raw integer format)
    sls_due_dt INT,                   -- Due date (raw integer format)
    sls_sales INT,                    -- Total sales amount
    sls_quantity INT,                 -- Quantity sold
    sls_price INT                     -- Unit selling price
);
GO

/* ============================================================
   Table: bronze.erm_cust_az12
   Purpose:
     Stores customer demographic attributes from ERM source AZ12.
   Layer:
     Bronze.
   Notes:
     - Used later to enrich CRM customer data.
     - May contain overlapping or conflicting attributes.
   ============================================================ */
IF OBJECT_ID('bronze.erm_cust_az12','U') IS NOT NULL
    DROP TABLE bronze.erm_cust_az12;

CREATE TABLE bronze.erm_cust_az12 (
    cid NVARCHAR(50),                 -- Customer identifier from ERM system
    bdate DATE,                       -- Customer birth date
    gen NVARCHAR(10)                  -- Gender from ERM source
);
GO

/* ============================================================
   Table: bronze.erm_loc_a101
   Purpose:
     Stores customer location data from ERM source A101.
   Layer:
     Bronze.
   Notes:
     - Country information used for geographic analysis.
   ============================================================ */
IF OBJECT_ID('bronze.erm_loc_a101','U') IS NOT NULL
    DROP TABLE bronze.erm_loc_a101;

CREATE TABLE bronze.erm_loc_a101 (
    cid NVARCHAR(50),                 -- Customer identifier
    cntry NVARCHAR(50)                -- Country name/code
);
GO

/* ============================================================
   Table: bronze.erm_px_cat_g1v2
   Purpose:
     Stores product category hierarchy and maintenance metadata.
   Layer:
     Bronze.
   Notes:
     - Category and subcategory mapping table.
     - Used later for dimensional modeling.
   ============================================================ */
IF OBJECT_ID('bronze.erm_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE bronze.erm_px_cat_g1v2;

CREATE TABLE bronze.erm_px_cat_g1v2 (
    id NVARCHAR(50),                  -- Product or category identifier
    category NVARCHAR(50),            -- High-level product category
    subcategory NVARCHAR(50),         -- Sub-category within category
    maintenance NVARCHAR(50)          -- Maintenance or status flag
);
GO
