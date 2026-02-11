/*
===============================================================================
Gold Layer Creation Script
===============================================================================
Description:
    This script defines the final views for the Gold Layer.
    It performs the following:
    1. Dimensions: Consolidates descriptive attributes (Customers, Products).
    2. Facts: Links business transactions (Sales) to the dimensions.
    3. Business Logic: Handles surrogate key generation, data integration 
       across sources, and filtering.
===============================================================================
*/

-------------------------------------------------------------------------------
-- 1. Create Customer Dimension
-------------------------------------------------------------------------------
PRINT '>>> Creating View: gold.dim_customer';
GO

CREATE VIEW gold.dim_customer AS (
    SELECT
        -- Generate a unique surrogate key for the Data Warehouse
        ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname AS last_name,
        ca.bdate AS birthdate,
        ci.cst_marital_status AS marital_status,
        -- Priority logic for Gender: CRM takes precedence over ERP data
        CASE 
            WHEN ci.cst_gender != 'N/A' THEN ci.cst_gender
            ELSE COALESCE(ca.gen, 'N/A')
        END AS gender,
        la.cntry AS country,
        ci.cst_create_date AS create_date
    FROM silver.crm_cust_info AS ci
    -- Join with ERP data for extended attributes (birthdate, gender)
    LEFT JOIN silver.erm_cust_az12 AS ca
        ON ca.cid = ci.cst_key
    -- Join with Location data for geographic analysis
    LEFT JOIN silver.erm_loc_a101 AS la
        ON la.cid = ci.cst_key
);

-------------------------------------------------------------------------------
-- 2. Create Product Dimension
-------------------------------------------------------------------------------
PRINT '>>> Creating View: gold.dim_products';
GO

CREATE VIEW gold.dim_products AS (
    SELECT
        -- Generate a unique surrogate key based on product timeline
        ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
        pn.prd_id AS product_id,
        pn.prd_key AS product_number,
        pn.prd_nm AS product_name,
        pn.cat_id AS category_id,
        pc.category AS category,
        pc.subcategory AS subcategory,
        pc.maintenance AS maintenance,
        pn.prd_cost AS product_cost,
        pn.prd_line AS product_line,
        pn.prd_start_dt AS product_start_date
    FROM silver.crm_prd_info AS pn
    -- Join with ERP Category table to get descriptive hierarchy
    LEFT JOIN silver.erm_px_cat_g1v2 AS pc
        ON pn.cat_id = pc.id
    -- Only include active products (Current State)
    WHERE pn.prd_end_dt IS NULL
);

-------------------------------------------------------------------------------
-- 3. Create Sales Fact Table
-------------------------------------------------------------------------------
PRINT '>>> Creating View: gold.fact_sales';
GO

CREATE VIEW gold.fact_sales AS (
    SELECT
        sd.sls_ord_num AS order_number,
        -- Link to Dim Product using the surrogate key
        pr.product_key AS product_key,
        -- Link to Dim Customer using the surrogate key
        cu.customer_key AS customer_key,
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt AS shipping_date,
        sd.sls_due_dt AS due_date,
        sd.sls_price AS price,
        sd.sls_quantity AS quantity,
        sd.sls_sales AS sales
    FROM silver.crm_sales_details AS sd
    -- Join to Gold Dimensions to replace natural keys with surrogate keys
    LEFT JOIN gold.dim_products AS pr
        -- Handle formatting differences between sales and product IDs
        ON sd.sls_prd_key = REPLACE(pr.product_number, '_', '-')
    LEFT JOIN gold.dim_customer AS cu
        ON sd.sls_cust_id = cu.customer_id
);

PRINT '>>> Gold Layer Views Created Successfully.';