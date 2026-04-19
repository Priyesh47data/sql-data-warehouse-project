 --======================================
          -- Created dimension customers
 --======================================
 
 
 CREATE VIEW gold.dim_customers AS
	SELECT
		ROW_NUMBER() OVER(ORDER BY cst_id) customer_key, --Surrogate Key
		ci.cst_id customer_id,
		ci.cst_key customer_number,
		ci.cst_firstname first_name,
		ci.cst_lastname last_name,
		cl.cntry country,
		ci.cst_marital_status marital_status,
		CASE
		WHEN ci.cst_gndr ! = 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'n/a')
		END gender,
		ca.bdate birthdate,
		ci.cst_create_date create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 cl
	ON ci.cst_key = cl.cid
	  



SELECT DISTINCT 
	ci.cst_gndr,
	ca.gen,
	CASE
		WHEN ci.cst_gndr ! = 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'n/a')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
Order By ci.cst_gndr,ca.gen



 --======================================
          -- Created dimension products
 --======================================

 CREATE VIEW gold.dim_products AS
	 SELECT 
		 ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) product_key, --Surrogate Key
		 pn.prd_id product_id,
		 pn.prd_key product_number,
		 pn.prd_nm product_name,
		 pn.cat_id category_id,
		 pc.cat category,
		 pc.subcat subcategory,
		 pc.maintenance,
		 pn.prd_cost cost,
		 pn.prd_line product_line,
		 pn.prd_start_dt AS start_date 
	 FROM silver.crm_prd_info pn
	 LEFT JOIN silver.erp_px_cat_g1v2 pc
		ON pn.cat_id = pc.id
	 Where pn.prd_end_dt IS NULL -- filter out alll historical data
  

   --======================================
          -- Created fact sales
 --======================================

CREATE VIEW gold.fact_sales AS
	SELECT
		 sls_ord_num order_number,
		 pr.product_key,
		 cu.customer_key,
		 sd.sls_order_dt order_date,
		 sd.sls_ship_dt shipping_date,
		 sd.sls_due_dt due_date,
		 sd.sls_sales sales_amount,
		 sd.sls_quantity quantity,
		 sd.sls_price price
	 FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_products pr
		ON sd.sls_prd_key = pr.product_number
	LEFT JOIN gold.dim_customers cu
		ON sd.sls_cust_id = cu.customer_id
  
  select top 1000 * from silver.crm_sales_details
  select top 10 * from gold.dim_products
   select top 10 * from silver.crm_prd_info
  select top 10 * from gold.dim_customers


 --======================================
     -- Foreign Key Integrity(Dimensions) - Checks if all dimension tables can successfully join to the fact tables
 --======================================

 SELECT *
 FROM gold.fact_sales fs
 LEFT JOIN gold.dim_customers dc
 ON fs.customer_key = dc.customer_key
 LEFT JOIN gold.dim_products dp
 ON fs.product_key = dp.product_key


