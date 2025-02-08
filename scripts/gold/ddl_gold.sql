/*
======================================================================================================
DDL Script: Creating Gold Views
======================================================================================================
Purpose:
      -This script is designed to generate views for the Gold layer in the data warehouse. The Gold layer serves as the final stage, where dimension and fact tables (following the Star Schema) are structured for analysis.

      -Each view integrates and transforms data from the Silver layer, ensuring a refined, enriched, and business-ready dataset.

Usage:
     -Provides a structured and optimized dataset, ready for analytical and reporting purposes.
======================================================================================================
*/

-- ======================================================================================================
--Cerate Dimension: gold.dim_customers
-- ======================================================================================================

IF OBEJCT_ID('gold.dim_customer', 'V') IS NOT NULL
    DROP VIEW gold.dim_customer;
Go

CREATE VIEW gold.dim_customer AS
SELECT
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name ,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --CRM is the MASter fro gender info.
     ELSE COALESCE(ca.gen, 'n/a')
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON   ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON   ci.cst_key=la.cid


GO
IF OBEJCT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS
SELECT
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory ,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date
FROM silver.crm_prd_info as pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id=pc.id
WHERE prd_end_date IS NULL --Filter out all historical data

GO

IF OBEJCT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS orde_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity, 
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu
ON sd.sls_cust_id =cu.customer_id


