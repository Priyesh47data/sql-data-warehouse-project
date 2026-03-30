
--Chesked ,is there any record duplicate
SELECT
*
FROM (
	SELECT 
	*,
	ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date DESC) FlagLast
	FROM silver.crm_cust_info
)t WHERE FlagLast > 1 

--Checks Unwanted spaces in name

SELECT
cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT
cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

--


 Select 
 prd_nm
 From silver.crm_prd_info
 --Where LEN(prd_nm) != LEN(TRIM(prd_nm))
              --OR
 Where prd_nm != TRIM(prd_nm)


 Select
  prd_cost
 From silver.crm_prd_info
 WHERE prd_cost < 0 OR prd_cost IS NULL
  
  Select * From silver.crm_prd_info
  Where prd_end_dt < prd_start_dt


--


--Ckecking null or invalide dates
Select   * 
From silver.crm_sales_details
Where sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
	OR sls_order_dt >20250330 OR sls_order_dt < 20060925
    OR  sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8
	OR  sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 


	--Checking null,0,or invalid values(less then 0 price/sales)in price ,sales ,quantity
Select  
	sls_quantity,
	sls_price,
	sls_sales
From silver.crm_sales_details
Where sls_sales != sls_price * sls_quantity
	OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
	OR sls_sales <=0 Or sls_quantity <=0 OR sls_price <=0



	--Checking in valid dates
	Select DISTINCT
	sls_order_dt
	From silver.crm_sales_details
	Where sls_order_dt IS NULL OR sls_order_dt  = 0 OR LEN(sls_order_dt) != 8 
		 

Select
	cat
From silver.erp_px_cat_g1v2
Where cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

    --Data Standardization & Consistencyc
Select Distinct
	subcat
From silver.erp_px_cat_g1v2
