/*
====================================================
  Bulk insert in CRM and ERP Tables and Creared STORED PROCEDURE to get data indertion time 
===================================================
*/
 						        /*===============================
								  BULK INSERT IN - CRM Table 
								 ================================*/
 

 CREATE OR ALTER PROCEDURE bronze.load_bronze AS
 BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@b_start_time DATETIME,@b_end_time DATETIME
	BEGIN TRY
	     SET @b_start_time = GETDATE()
		 PRINT '====================================';
		 PRINT 'LOADING Bronze Layer';
		 PRINT '====================================';

		 PRINT '------------------------------------';
		 PRINT 'LOADING CRM Tables';
		 PRINT '------------------------------------';
		 SET @start_time = GETDATE();
 		 PRINT '>> Truncating Table: bronze.crm_cust_info';
 		 TRUNCATE TABLE bronze.crm_cust_info; 
		 PRINT '>>Inserting Data Into: bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\priye\Desktop\🦑_Srs\SQL With BARRA\🦑_Srs_Proj\datasets\source_crm\cust_info.csv'
		WITH
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
	   SET @end_time = GETDATE();
	   PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secons';
	   PRINT '>> --------------------';

	    SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
 		PRINT '>>Inserting Data Into: bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\priye\Desktop\🦑_Srs\SQL With BARRA\🦑_Srs_Proj\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
       SET @end_time = GETDATE();
	   PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secons';
	   PRINT '>> --------------------';

	    SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
  		PRINT '>>Inserting Data Into: bronze.crm_sales_details';

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\priye\Desktop\🦑_Srs\SQL With BARRA\🦑_Srs_Proj\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
	    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secons';
	    PRINT '>> --------------------';
								 /*===============================
										  BULK INSERT IN - ERP Table 
										 ================================*/
		 PRINT '------------------------------------';
		 PRINT 'LOADING ERP Tables';
		 PRINT '------------------------------------';
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12'; 
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>>Inserting Data Into: bronze.erp_cust_az12';


		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\priye\Desktop\🦑_Srs\SQL With BARRA\🦑_Srs_Proj\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
	    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secons';
	    PRINT '>> --------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101'; 
		TRUNCATE TABLE bronze.erp_loc_a101;
 		PRINT '>>Inserting Data Into: bronze.erp_loc_a101';


		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\priye\Desktop\🦑_Srs\SQL With BARRA\🦑_Srs_Proj\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
	    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secons';
	    PRINT '>> --------------------';

	    SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'; 
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
 		PRINT '>>Inserting Data Into: bronze.erp_px_cat_g1v2';


		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\priye\Desktop\🦑_Srs\SQL With BARRA\🦑_Srs_Proj\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
	    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secons';
	    PRINT '>> --------------------';

		SET @b_end_time = GETDATE()
		PRINT '========================================'
		PRINT 'Loading Bronze Layer is Completed'
        PRINT 'Load time of bronze: '  + CAST(DATEDIFF(second,@b_start_time,@b_end_time) AS NVARCHAR) + ' seconds';
		PRINT '========================================'

	END TRY
    BEGIN CATCH
		PRINT '=========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT '=========================================';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST( ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST( ERROR_STATE() AS NVARCHAR);  
	END CATCH
    
END;

Exec bronze.load_bronze









