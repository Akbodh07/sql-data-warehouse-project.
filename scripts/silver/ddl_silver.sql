/*
================================================
DDL SCRIPT : CREATING SILVER LAYER
================================================

PURPOSE : THIS SCRIPT CREATES TABLES IN THE silver schema , dropping the existing tables if they already exist.
Run this script to redifine the DDL structure of bronze tables
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS -- created stored procedure to load silver table
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
	PRINT '=========================================================';
	PRINT 'LOADING SILVER LAYER';
	PRINT '=========================================================';

	SET @start_time = GETDATE();

	PRINT 'TRUNCATING TABLE : silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT 'INSERTING DATA INTO TABLE : crm_cust_info';

	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,

	CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END as cst_marital_status,

	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END as cst_gndr, 
	cst_create_date
	FROM (

	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	)t WHERE flag_last = 1
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

		SET @start_time = GETDATE()
	PRINT 'TRUNCATING TABLE : silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT 'INSERTING DATA INTO TABLE : silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id, -- this column added after cleaning the bronze layer
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt, 
		prd_end_dt 

		)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, 
		-- link with erp_px_cat_g1v2 for categrorization

		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		-- link with sales prdouct details using prd_key

		prd_nm,

		ISNULL(prd_cost,0) AS prd_cost,  
		-- HANDLED NULL VALUES IF ANY

		CASE
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Moutain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line, 

		-- HANDLED STANDARDIZATION OF prd_line 
		CAST(prd_start_dt AS DATE) AS prd_start_dt,

		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) as prd_end_dt
		-- TO GET THE NEXT END DATE OF SAME PRODUCT USED LEAD

		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE : silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT 'INSERTING DATA INTO TABLE : silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details (
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt	,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price 
	)

	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,

		CASE
			WHEN sls_order_dt = 0 or LEN(sls_order_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,

		CASE
			WHEN sls_order_dt = 0 or LEN(sls_ship_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)

		END AS sls_ship_dt,

		CASE
			WHEN sls_order_dt = 0 or LEN(sls_due_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)

		END AS sls_due_dt,

		CASE 
				WHEN sls_sales IS NULL OR sls_sales < 0 OR sls_sales != ABS(sls_quantity) * ABS(sls_price) 
					THEN ABS(sls_quantity) * ABS(sls_price)
				ELSE sls_sales
	
		END AS sls_sales,

		ABS(sls_quantity) AS sls_quantity,
		ABS(sls_price) AS sls_price
		
	FROM bronze.crm_sales_details
	SET @end_time = GETDATE();
	PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE : silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT 'INSERTING DATA INTO TABLE : silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
	SELECT 
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END as cid,
	
		CASE 
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END as bdate,

		CASE
			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'n/a'
		END as gen
	FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE : silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT 'INSERTING DATA INTO TABLE : silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101(cid,cntry)
	SELECT 
		TRIM(REPLACE(cid,'-','')) AS cid,
		CASE 
			WHEN UPPER(TRIM(cntry)) IN ('US','United States') THEN 'USA'
			WHEN UPPER(TRIM(cntry)) IN ('de','DE') THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) = '' OR UPPER(TRIM(cntry))  IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END as cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE : silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT 'INSERTING DATA INTO TABLE : silver.erp_px_cat_g1v2';

	INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
	)
	SELECT 
			id,
			cat,
			subcat,
			maintenance
	FROM bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
	END TRY
BEGIN CATCH
	PRINT '===================================='
	PRINT 'ERROR OCCURED DURING LOADING BROZE LAYER'
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR )
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR )
END CATCH 
END
	
