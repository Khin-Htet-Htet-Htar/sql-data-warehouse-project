/*
===============================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY`command to load data from csv Files to bronze tables.

Parameters:
    None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
===============================================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
		batch_start_time TIMESTAMP := clock_timestamp();
		batch_end_time TIMESTAMP;
		start_time TIMESTAMP;
		end_time TIMESTAMP;
BEGIN
	BEGIN
	
		RAISE NOTICE '====================================================================================';
        RAISE NOTICE 'Loading Bronze Layer';
        RAISE NOTICE '====================================================================================';
	
		RAISE NOTICE '------------------------------------------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '------------------------------------------------------------------------------------';


		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
        EXECUTE 'COPY bronze.crm_cust_info FROM E''D:\\SQL_DataWarehouse_Project_fromDatawithBaraa\\datasets\\source_crm\\cust_info.csv'' WITH (FORMAT CSV, HEADER true, DELIMITER '','')';
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)));


		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
		EXECUTE 'COPY bronze.crm_prd_info FROM E''D:\\SQL_DataWarehouse_Project_fromDatawithBaraa\\datasets\\source_crm\\prd_info.csv'' WITH ( FORMAT CSV, HEADER true, DELIMITER '','')';
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)));


		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
		EXECUTE 'COPY bronze.crm_sales_details FROM E''D:\\SQL_DataWarehouse_Project_fromDatawithBaraa\\datasets\\source_crm\\sales_details.csv'' WITH ( FORMAT CSV, HEADER true, DELIMITER '','')';
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)));
		

		RAISE NOTICE '------------------------------------------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '------------------------------------------------------------------------------------';


		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
		EXECUTE 'COPY bronze.erp_CUST_AZ12 FROM E''D:\\SQL_DataWarehouse_Project_fromDatawithBaraa\\datasets\\source_erp\\CUST_AZ12.csv'' WITH ( FORMAT CSV, HEADER true, DELIMITER '','')';
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)));
		

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
		EXECUTE 'COPY bronze.erp_LOC_A101 FROM E''D:\\SQL_DataWarehouse_Project_fromDatawithBaraa\\datasets\\source_erp\\LOC_A101.csv'' WITH ( FORMAT CSV, HEADER true, DELIMITER '','')';
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)));
		

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		EXECUTE 'COPY bronze.erp_PX_CAT_G1V2 FROM E''D:\\SQL_DataWarehouse_Project_fromDatawithBaraa\\datasets\\source_erp\\PX_CAT_G1V2.csv'' WITH ( FORMAT CSV, HEADER true, DELIMITER '','')';
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)));


		RAISE NOTICE '====================================================================================';
        RAISE NOTICE 'Bronze Layer Load Completed Successfully!';
        RAISE NOTICE '====================================================================================';


		EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE '====================================================================================';
		RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		RAISE NOTICE 'Error Message: %', SQLERRM;
		RAISE NOTICE 'Error State: %', SQLSTATE;
		RAISE NOTICE '====================================================================================';
	END;
	batch_end_time := clock_timestamp();
	RAISE NOTICE '>> Total Batch Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (batch_end_time - batch_start_time)));
END;
$$;
