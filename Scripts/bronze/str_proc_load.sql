CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	TRUNCATE TABLE bronze.crm_cust_info
	BULK INSERT bronze.crm_cust_info

	FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
	TRUNCATE TABLE bronze.crm_prd_info
	BULK INSERT bronze.crm_prd_info

	FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
	TRUNCATE TABLE bronze.crm_sales_details
	BULK INSERT bronze.crm_sales_details

	FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
	TRUNCATE TABLE bronze.erp_cust_az12
	BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
	TRUNCATE TABLE bronze.erp_loc_a101
	BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK);
	TRUNCATE TABLE bronze.erp_px_cat_g1v2
	BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Azubuike Orioha\Desktop\DATA SCIENCE\00001 SQL PROJECT\DataSet\PX_CAT_G1V2.csv'
			WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
END

