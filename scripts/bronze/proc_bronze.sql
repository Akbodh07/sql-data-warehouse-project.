
-- CREATED A STORED PROCEDURE TO LOAD DATA EASILY
-- EXECUTE bronze.load_bronze;
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;

	SET @batch_start_time = GETDATE();
	BEGIN TRY
	PRINT '=========================================================';
	PRINT 'LOADING BROZE LAYER';
	PRINT '=========================================================';

	SET @start_time = GETDATE();
	PRINT '>> TRUNCATING TABLE : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>INSERTING TABLE : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\ak files\my desktop data\sql practice data\DATA WAREHOUSE PROJECT\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> INSERTING TABLE : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\ak files\my desktop data\sql practice data\DATA WAREHOUSE PROJECT\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
		SET @end_time = GETDATE();
		PRINT 'Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> INSERTING TABLE : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\ak files\my desktop data\sql practice data\DATA WAREHOUSE PROJECT\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK ) ;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> INSERTING TABLE : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\ak files\my desktop data\sql practice data\DATA WAREHOUSE PROJECT\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK )
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> INSERTING TABLE : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\ak files\my desktop data\sql practice data\DATA WAREHOUSE PROJECT\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK )
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT 'INSERTING TABLE : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\ak files\my desktop data\sql practice data\DATA WAREHOUSE PROJECT\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK );
		SET @end_time= GETDATE();

		PRINT 'Loading bronze layer is completed';
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + 'seconds';
		SET @batch_end_time = GETDATE();
		PRINT '>> TOTAL DURATION TO LOAD BRONZE LAYER IS :' + CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
END TRY
BEGIN CATCH
	PRINT '===================================='
	PRINT 'ERROR OCCURED DURING LOADING BROZE LAYER'
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR )
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR )
END CATCH 
END

