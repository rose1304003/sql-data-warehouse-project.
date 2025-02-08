/*
==========================================================================================
Quality Checks
==========================================================================================

Script Purpose:
This script performs quality checks to validate the integrity, consistency, 
and accuracy of the Gold Layer. These checks ensure:
- Uniqueness of surrogate keys in dimension tables.
- Referential integrity between fact and dimension tables.
- Validation of relationships in the data model for analytical purposes.

Usage Notes:
- Run these checks after data loading Silver Layer.
- Investigate and resolve any discrepancies found during the checks.
*/

-- ========================================================================================
-- Checking 'gold.dim_customers'
-- ========================================================================================

-- Check for uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*)>1;

-- ========================================================================================
--Checking 'gold.dim_product'
-- ========================================================================================
--Check for uniquesness of Product Key in gold.dim_product
--Expectation: No results
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY customer_key
HAVING COUNT(*)>1;

-- ========================================================================================
--Checking 'gold.fact_sales'
-- ========================================================================================
--Check the data model conncetivity bewtween fact and dimensions.
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
On c.customer_key = f.customer_key
LEFT JOIN gold.dim_product p
On p.product_key = f.product_key
WHERE p.rpoduct_key IS NULL OR c.customer_key IS NULL

