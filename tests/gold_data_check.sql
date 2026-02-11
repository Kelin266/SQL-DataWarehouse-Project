/*
===============================================================================
Data Validation and Quality Assurance Script
===============================================================================
Purpose:
    1. Validate business logic for field transformations (e.g., Gender COALESCE).
    2. Inspect the final output of the Gold layer views.
    3. Perform Referential Integrity checks to ensure Fact records have 
       corresponding Dimension entries.
===============================================================================
*/

-------------------------------------------------------------------------------
-- 1. Logic Validation: Gender Mapping
-------------------------------------------------------------------------------
-- Purpose: Verify that the CASE statement correctly prioritizes CRM data 
-- and fills gaps using ERP data.
PRINT '>>> Validating Gender Transformation Logic...';

SELECT DISTINCT
    ci.cst_gender AS crm_gender,
    ca.gen AS erp_gender,
    CASE 
        WHEN ci.cst_gender != 'N/A' THEN ci.cst_gender -- CRM is the master for gender info
        ELSE COALESCE(ca.gen, 'N/A')
    END AS final_derived_gender
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erm_cust_az12 AS ca
    ON ca.cid = ci.cst_key
LEFT JOIN silver.erm_loc_a101 AS la
    ON la.cid = ci.cst_key
ORDER BY 1, 2;

-------------------------------------------------------------------------------
-- 2. Final View Inspection
-------------------------------------------------------------------------------
-- Purpose: Preview the final state of the transformed data.
PRINT '>>> Inspecting Gold Layer Views...';

-- Check Customer Dimension
PRINT ' - Previewing gold.dim_customer';
SELECT * FROM gold.dim_customer;

-- Check Product Dimension
PRINT ' - Previewing gold.dim_products';
SELECT * FROM gold.dim_products;

-- Check Sales Fact
PRINT ' - Previewing gold.fact_sales';
SELECT * FROM gold.fact_sales;

-------------------------------------------------------------------------------
-- 3. Referential Integrity Check (Foreign Key Validation)
-------------------------------------------------------------------------------
-- Purpose: Identify any "Orphaned" records in the Fact table.
-- If this query returns rows, it means sales exist for customers or products 
-- that are missing from the dimensions.
PRINT '>>> Checking Foreign Key Integrity (Fact-to-Dimension)...';

SELECT 
    f.order_number,
    f.customer_key,
    f.product_key,
    c.customer_id AS matched_customer,
    p.product_id AS matched_product
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customer AS c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products AS p
    ON p.product_key = f.product_key
WHERE c.customer_key IS NULL  -- Identify missing Customers
   OR p.product_key IS NULL;  -- Identify missing Products

PRINT '>>> Validation Complete.';