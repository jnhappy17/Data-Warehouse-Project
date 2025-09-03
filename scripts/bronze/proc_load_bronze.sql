/*
Stored procedure
This stored procedure loads data into the 'bronze' schema layer from external CSV files, using BULK INSERT command after truncating the tables.
It does not accept any parameters or return any value.

Example:
	EXEC bronze.load_bronze
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT ' Loading CRM Tables in Bronze layer';
		-- Insert data in table bronze.crm_cust_info
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Portfolio\sql-dwh-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT ' >> Truncate and insert data into bronze.crm_cust_info';

		-- Insert data in table bronze.crm_prd_info
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Portfolio\sql-dwh-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT ' >> Truncate and insert data into bronze.crm_prd_info';

		-- Insert data in table bronze.crm_sales_details
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Portfolio\sql-dwh-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT ' >> Truncate and insert data into bronze.crm_sales_details';

		-- Empty space for cleanliness
		PRINT ' ';
		PRINT ' ';

		PRINT ' Loading ERP Tables in Bronze layer';
		-- Insert data in table bronze.erp_cust_az12
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Portfolio\sql-dwh-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT ' >> Truncate and insert data into bronze.erp_cust_az12';

		-- Insert data in table bronze.erp_loc_a101
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Portfolio\sql-dwh-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT ' >> Truncate and insert data into bronze.erp_loc_a101';

		-- Insert data in table bronze.erp_px_cat_g1v2
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Portfolio\sql-dwh-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT ' >> Truncate and insert data into bronze.erp_px_cat_g1v2';
		SET @batch_end_time = GETDATE();

		-- Empty space for cleanliness
		PRINT ' ';
		PRINT ' ';
		PRINT 'Loading Bronze Layer Completed'
		PRINT 'Total load Duration:' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	END TRY

	BEGIN CATCH
		PRINT '------------------------------------------';
		PRINT 'ERROR OCCURED DURING BRONZE LAYER LOADING';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '------------------------------------------';
	END CATCH
END
