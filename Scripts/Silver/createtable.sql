-- check if the table already exists, if so drop it
IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL 
Drop TABLE silver.crm_cust_info

-- create table silver.crm_cust_info
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE)
	dwh_create_date DATETME2 DEFAULT GETDATE();



-- check if the table already exists, if so drop it
IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL 
Drop TABLE silver.crm_sales_details

-- create table silver.crm_sales_details
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_id INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT)
	dwh_create_date DATETME2 DEFAULT GETDATE();



-- check if the table already exists, if so drop it
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL 
Drop TABLE silver.crm_prd_info

	
-- create table silver.crm_prd_info
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,)
	dwh_create_date DATETME2 DEFAULT GETDATE();


-- check if the table already exists, if so drop it
IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
Drop TABLE silver.erp_loc_a101

-- create table silver.erp_loc_a101	
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50))
	dwh_create_date DATETME2 DEFAULT GETDATE();


-- check if the table already exists, if so drop it
IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
Drop TABLE silver.erp_cust_az12

-- create table silver.erp_cust_az12
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50))
	dwh_create_date DATETME2 DEFAULT GETDATE();


-- check if the table already exists, if so drop it
IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
Drop TABLE silver.erp_px_cat_g1v2

-- create table silver.erp_px_cat_g1v2
CREATE TABLE silver.erp_px_cat_g1v2(
	cid NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50))
	dwh_create_date DATETME2 DEFAULT GETDATE();
