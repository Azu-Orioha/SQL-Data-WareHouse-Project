/*
================================================================================
PROCEDURE: bronze.load_bronze
PURPOSE: Loads data from CSV files into bronze layer tables in the database.
         Handles both CRM (cust_info, prd_info, sales_details) and ERP 
         (cust_az12, loc_a101, px_cat_g1v2) source systems.
OPERATION:
    - Truncates target tables before loading
    - Uses BULK INSERT for efficient data loading
    - Provides detailed logging of operation timing
    - Includes error handling for load failures
DEPENDENCIES: 
    - CSV files must exist in specified paths
    - Target tables must exist in bronze schema
AUTHOR: Azubuike Orioha
CREATED: [26-March 2025]
================================================================================
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=====================================================';
		PRINT '				LOADING THE BRONZE LAYER';
		PRINT '======================================================';
	
	
		PRINT '-------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		
		PRINT '>>>> Inserting Data into Table bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		PRINT '>>>> Truncating Table bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>>> Inserting Data into Table bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\prd_info.csv'
		
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);



		PRINT '>>>> Truncating Table bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>>> Inserting Data into Table bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>>>>>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		


		PRINT '-------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------';



		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		PRINT '>>>> Inserting Data into Table bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\CUST_AZ12.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		
		);
		
		PRINT '>>>> Truncating Table bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>>> Inserting Data into Table bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\LOC_A101.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		
		
		PRINT '>>>> Truncating Table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>>> Inserting Data into Table bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

		SET @end_time = GETDATE();
		PRINT '>>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------------';
	
	
		SET @batch_end_time = GETDATE();
		PRINT '========================================================';
		PRINT '				LOADING BRONZE LAYER IS COMPLETED'
		PRINT '             Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '========================================================';
	END TRY
	BEGIN CATCH
		PRINT '===============================';
		PRINT 'ERROR OCCURED DURING LOADING BRONE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===============================';
	END CATCH
END
