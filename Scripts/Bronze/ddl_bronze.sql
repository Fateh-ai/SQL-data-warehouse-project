/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


-- create Database 'Datawarehouse'
Use master;

Create Database Datawarehouse;

Use Datawarehouse;
go

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Bronze')
    EXEC('CREATE SCHEMA [Bronze]');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Sliver')  
    EXEC('CREATE SCHEMA [Sliver]');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Gold')
    EXEC('CREATE SCHEMA [Gold]');
GO


IF OBJECT_ID('Bronze.crm_cust_info', 'U') IS NOT NULL
DROP TABLE Bronze.crm_cust_info;
CREATE TABLE Bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50), 
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

IF OBJECT_ID('Bronze.crm_prd_info', 'U') IS NOT NULL
DROP TABLE Bronze.crm_prd_info;
CREATE TABLE Bronze.crm_prd_info(
prd_id INT,
prd_key Nvarchar(50),
prd_nm Nvarchar(50),
prd_cost INT, 
prd_line Nvarchar(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);


IF OBJECT_ID('Bronze.crm_sales_deatails', 'U') IS NOT NULL
DROP TABLE Bronze.crm_sales_deatails;
CREATE TABLE Bronze.crm_sales_deatails(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
);


IF OBJECT_ID('Bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE Bronze.erp_loc_a101;
CREATE TABLE Bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);

IF OBJECT_ID('Bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE Bronze.erp_cust_az12;
CREATE TABLE Bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);

IF OBJECT_ID('Bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE Bronze.erp_px_cat_g1v2;
CREATE TABLE Bronze.erp_px_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintence VARCHAR(50)
);



GO

CREATE OR ALTER PROCEDURE Bronze.load_Bronze AS
BEGIN
	DECLARE @start_time DATETIME, @End_time DATETIME, @batch_start_time DATETIME, @Batch_End_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'LOADING BRONZE LAYER'
		PRINT '==========================================';
	
		PRINT '------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '-------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING TABLE :  Bronze.crm_cust_info';
		TRUNCATE TABLE  Bronze.crm_cust_info;
		PRINT 'INSERTING DATA INTO: Bronze.crm_cust_info';
		BULK INSERT  Bronze.crm_cust_info
		From 'E:\BA\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '------------------------'
	

		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING TABLE :  Bronze.crm_prd_info';
		TRUNCATE TABLE  Bronze.crm_prd_info;
		PRINT 'INSERTING DATA INTO: Bronze.crm_prd_info';
		BULK INSERT Bronze.crm_prd_info
		From 'E:\BA\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '------------------------'


		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING TABLE :   Bronze.crm_sales_deatails';
		TRUNCATE TABLE  Bronze.crm_sales_deatails;
		PRINT 'INSERTING DATA INTO: Bronze.crm_sales_deatails';
		BULK INSERT  Bronze.crm_sales_deatails
		From 'E:\BA\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '------------------------'


		
		PRINT '------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '-------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING TABLE :  Bronze.erp_loc_a101';
		TRUNCATE TABLE  Bronze.erp_loc_a101;
		PRINT 'INSERTING DATA INTO: Bronze.erp_loc_a101';
		BULK INSERT Bronze.erp_loc_a101
		From 'E:\BA\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '------------------------'


		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING TABLE :  Bronze.erp_cust_az12';
		TRUNCATE TABLE  Bronze.erp_cust_az12;
		PRINT 'INSERTING DATA INTO:  Bronze.erp_cust_az12';
		BULK INSERT Bronze.erp_cust_az12
		From 'E:\BA\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '------------------------'


		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING TABLE :  Bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE  Bronze.erp_px_cat_g1v2;
		PRINT 'INSERTING DATA INTO: Bronze.erp_px_cat_g1v2';
		BULK INSERT Bronze.erp_px_cat_g1v2
		From 'E:\BA\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);	
		SET @End_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '------------------------'

		SET @Batch_End_time = GETDATE();
		PRINT '=======================================================';
		PRINT 'LOADING BRONZE LAYER IS COMPLETED'
		PRINT '>> TOTAL LOAD DURATION:' + CAST(DATEDIFF(SECOND, @batch_start_time, @Batch_end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '------------------------'
		PRINT '=======================================================';
		END TRY
		BEGIN CATCH 
		PRINT '================================================';
		PRINT 'ERROR OCCURED DURING ;OADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';
		END CATCH 
END;

GO  

EXEC Bronze.load_Bronze
