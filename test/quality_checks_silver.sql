
/*
===================================================================================
Quality Checks
===================================================================================

Script Purpose:
      This scripts performs various quality checks for date consistency, acuracy,
      and standardized across the 'silver' schemas. It includes checks for : 
      - Null or duplicate primary keys.
      - Unwanted spaces in string fields.
      - Data standardization and consistency.
      - Invalid date ranges and orders.
      - Data consistency between related fileds:

Usage Notes:
      - Run these checks after data loading Silver Layer.
      - Investigate and resolve any discrepanices found during the checks.

===================================================================================
*/



-- =================================================================================
--Checking 'silver.crm_cust_info'
--Check for unwanted Spaces
--Expectation: No Results
SELECT 
cst_firstname,
cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Data Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info 

--Data Consistency
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info 

-- Identfying duplicate values
SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL

-- =================================================================================
--Checking 'silver.crm_prd_info'
--Check for unwanted Spaces
--Expectation: No Results
SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

------------------------------------------------------------
--Check for unwanted Spaces
--Expectation: No Results
SELECT 
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

------------------------------------------------------------
--Check for NULLS pr Negative Numbers
--Expectation: No Results
SELECT prd_cost 
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

------------------------------------------------------------
--Data Standarzation & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

 ------------------------------------------------------------ 
--Check for Invlaid Date Orders
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT 
      prd_key,
	  prd_id,
	  prd_nm,
	  prd_start_dt,
	  prd_end_dt,
	  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-Hl-U509')


-- =================================================================================
--Checking 'silver.crm_sales_details'
--Check for unwanted Spaces
--Expectation: No Results
  SELECT *  
FROM silver.crm_sales_details
WHERE sls_prd_key = TRIM(sls_prd_key)  
    OR sls_prd_key != TRIM(sls_prd_key); 

------------------------------------------------------------  
--Check for Invlaid Date Orders
SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 OR
sls_order_dt > 20500101

SELECT 
* 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

------------------------------------------------------------  
--Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative

SELECT DISTINCT
sls_sales as old_sls_sales,
sls_price as old_sls_price,
sls_quantity as old_sls_quantity,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
	 ElSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
     THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
END as sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity *sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <= 0 OR sls_price <=0
ORDER BY sls_sales,
sls_price,
sls_quantity


-- =================================================================================
--Checking 'silver.erp_cust_az12'
------------------------------------------------------------  
--Indetify Out-Of-Range Dates
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

------------------------------------------------------------  
--Data Standarzation & Consistency
SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12


-- =================================================================================
--Checking 'silver.erp_loc_a101'
------------------------------------------------------------   
-- Data Standardization & Consistency  
SELECT DISTINCT  
    cntry  
FROM silver.erp_loc_a101  
ORDER BY cntry;  


-- ================================================================================= 
-- Checking ‘silver.erp_px_cat_glv2’  
------------------------------------------------------------  
-- Check for Unwanted Spaces  
-- Expectation: No Results  
SELECT *  
FROM silver.erp_px_cat_glv2  
WHERE cat = TRIM(cat)  
    OR cat != TRIM(cat;  

------------------------------------------------------------  
-- Data Standardization & Consistency  
SELECT DISTINCT  
FROM silver.erp_px_cat_glv2;  
