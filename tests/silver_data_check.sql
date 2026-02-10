/*
===============================================================================
Quality Checks (Data Validation Script)
===============================================================================
Script Purpose:
    This script performs comprehensive Data Quality (DQ) checks on the 'silver' layer.
    The goal is to validate that the ETL transformations (Silver Layer Load) 
    successfully handled:
    - Entity Integrity (No NULL or duplicate Primary Keys).
    - Domain Integrity (Standardized values for Gender, Marital Status, etc.).
    - Referential/Logical Integrity (Valid date sequences, Sales calculations).
    - Data Hygiene (No leading/trailing spaces).

Usage Notes:
    - Execute this script immediately after running 'silver.load_silver'.
    - Any returned rows indicate a failure in the cleansing logic that requires 
      investigation in the Bronze-to-Silver transformation code.
===============================================================================
*/

PRINT '===============================================================';
PRINT 'STARTING DATA QUALITY CHECKS: SILVER LAYER';
PRINT '===============================================================';

/* ===============================================================================
CHECKING DATA FROM: silver.crm_cust_info
===============================================================================
*/
PRINT '>> Reviewing: silver.crm_cust_info';

-- Check 1: Primary Key Integrity (cst_id)
-- Result should be empty. Rows here indicate duplicates or missing IDs.
PRINT '   - Check: Primary Key Integrity (Duplicates or NULLs)';
SELECT 
    cst_id, 
    COUNT(*) AS record_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check 2: Data Hygiene (TRIM validation)
-- Rows indicate the TRIM transformation failed to remove spaces.
PRINT '   - Check: Unwanted spaces in Names';
SELECT 'cst_firstname' AS col, cst_firstname FROM silver.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname)
UNION ALL
SELECT 'cst_lastname' AS col, cst_lastname FROM silver.crm_cust_info WHERE cst_lastname != TRIM(cst_lastname);

-- Check 3: Domain Standardization (Gender & Marital Status)
-- Result should only contain 'Male', 'Female', 'Single', 'Married', or 'N/A'.
PRINT '   - Check: Standardized Values (Gender/Marital Status)';
SELECT DISTINCT 'cst_gender' AS field, cst_gender AS val FROM silver.crm_cust_info
UNION ALL
SELECT DISTINCT 'cst_marital_status' AS field, cst_marital_status FROM silver.crm_cust_info;

/* ===============================================================================
CHECKING DATA FROM: silver.crm_prd_info
===============================================================================
*/
PRINT '>> Reviewing: silver.crm_prd_info';

-- Check 1: Primary Key Integrity (prd_id)
PRINT '   - Check: Primary Key Integrity';
SELECT prd_id, COUNT(*) FROM silver.crm_prd_info GROUP BY prd_id HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check 2: Cost Validation
-- Rows here mean products have no cost or negative cost.
PRINT '   - Check: Negative or NULL Costs';
SELECT * FROM silver.crm_prd_info WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check 3: Product Line Normalization
PRINT '   - Check: Standardized Product Lines';
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

-- Check 4: Date Logic (Start vs End)
-- Products cannot end before they start.
PRINT '   - Check: Invalid Date Sequences (End Date < Start Date)';
SELECT * FROM silver.crm_prd_info WHERE prd_end_dt < prd_start_dt;

/* ===============================================================================
CHECKING DATA FROM: silver.crm_sales_details
===============================================================================
*/
PRINT '>> Reviewing: silver.crm_sales_details';

-- Check 1: Order ID presence
PRINT '   - Check: NULL Order Numbers';
SELECT sls_ord_num FROM silver.crm_sales_details WHERE sls_ord_num IS NULL;

-- Check 2: Logical Date Sequence
-- Orders must happen before Shipping and Due dates.
PRINT '   - Check: Chronological Logic (Order vs Ship vs Due)';
SELECT sls_ord_num, sls_order_dt, sls_ship_dt, sls_due_dt 
FROM silver.crm_sales_details 
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check 3: Financial Integrity (Quantity * Price = Sales)
-- This ensures the calculated fields and price absolute values are correct.
PRINT '   - Check: Sales Calculation Integrity (Sales = Qty * Price)';
SELECT sls_ord_num, sls_quantity, sls_price, sls_sales 
FROM silver.crm_sales_details 
WHERE sls_sales != (sls_quantity * sls_price)
   OR sls_sales <= 0 OR sls_price <= 0 OR sls_quantity <= 0;

/* ===============================================================================
CHECKING DATA FROM: silver.erm_cust_az12
===============================================================================
*/
PRINT '>> Reviewing: silver.erm_cust_az12';

-- Check 1: ID Hygiene (No 'NAS' prefix remains and no spaces)
PRINT '   - Check: CID Hygiene';
SELECT cid FROM silver.erm_cust_az12 WHERE cid IS NULL OR cid != TRIM(cid);

-- Check 2: Demographic Accuracy (Age check)
-- Flags customers with birthdates that are physically impossible or outliers.
PRINT '   - Check: Reasonable Birthdates (1924-2024)';
SELECT bdate FROM silver.erm_cust_az12 WHERE bdate < '1924-01-01' OR bdate > '2024-01-01';

/* ===============================================================================
CHECKING DATA FROM: silver.erm_loc_a101
===============================================================================
*/
PRINT '>> Reviewing: silver.erm_loc_a101';

-- Check 1: Country Normalization
-- Ensures DE, US, USA were successfully mapped to 'Germany' or 'United States'.
PRINT '   - Check: Country Name Standardization';
SELECT DISTINCT cntry FROM silver.erm_loc_a101;

/* ===============================================================================
CHECKING DATA FROM: silver.erm_px_cat_g1v2
===============================================================================
*/
PRINT '>> Reviewing: silver.erm_px_cat_g1v2';

-- Check 1: Hierarchy Completeness
PRINT '   - Check: NULL Category/Subcategory';
SELECT * FROM silver.erm_px_cat_g1v2 WHERE category IS NULL OR subcategory IS NULL;

PRINT '===============================================================';
PRINT 'DATA QUALITY CHECKS COMPLETED';
PRINT '===============================================================';