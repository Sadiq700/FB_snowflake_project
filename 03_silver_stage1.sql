--Snowflake Supply Chain Project
--Script : 03_silver/silver_stage1.sql
--Purpose : Silver Stage 1 - STG_ORDERS table,
--         Task, MERGE, Dead Letter table
--Run as : SYSADMIN

--ENVIRONMENT
set env = 'DEV';;
set DB = 'FB_' || $env || '_DB';

use database identifier($DB);
use role sysadmin;
use schema silver_sch;
use warehouse fb_pipeline_wh;

--Step 1 - DEAD LETTER TABLE
--Transient - stores bad/rejected records
--Created first so Task can reference it
create or replace transient table silver_sch.dead_letter(
    raw_data variant,
    error_reason string,
    file_name string,
    file_row_number number,
    rejected_at timestamp_ntz default current_timestamp()
) comment = ' Dead Letter Table - rejected/bad records from bronze';

-- Step 2 - STG ORDERS TABLE
-- Permanent - flattened, typed, cleaned JSON
-- All columns from JSON schema included
-- Time travel enabled (default 1 day)

create or replace table silver_sch.stg_orders (

--Order Fields
 ORDER_ID  STRING  COMMENT 'Unique order identifier', 
ORDER_DATE TIMESTAMP_NTZ COMMENT 'Order timestamp', 
ORDER_STATUS STRING COMMENT 'Current order status',

--Customer fields
CUSTOMER_ID STRING COMMENT'Customer identifier',
CUSTOMER_NAME STRING COMMENT 'Customer name',
CUSTOMER_REGION STRING COMMENT 'Customer region',
CUSTOMER_SEGMENT STRING COMMENT 'Customer segment' ,

--Supplier fields
SUPPLIER_ID STRING COMMENT 'Supplier identifier',
SUPPLIER_NAME STRING COMMENT 'Supplier name',
SUPPLIER_COUNTRY STRING COMMENT 'Supplier country',
LEAD_TIME_DAYS NUMBER COMMENT 'Supplier lead time in days',
PERFORMANCE_SCORE FLOAT COMMENT 'Supplier performance score',

-- Shipment fields
SHIPMENT_ID STRING COMMENT 'Shipment identifier',
CARRIER STRING COMMENT 'Shipping carrier',
SHIP_DATE TIMESTAMP_NTZ COMMENT 'Shipment date',
ESTIMATED_DELIVERY DATE COMMENT 'Estimated delivery date',
DELAY_DAYS NUMBER COMMENT 'Number of delay days',

--Product fields (flattened from items array)
PRODUCT_ID STRING COMMENT 'Product identifier',
PRODUCT_NAME  STRING COMMENT 'Product name',
CATEGORY   STRING COMMENT 'Product category',
QUANTITY NUMBER COMMENT 'Order quantity',
UNIT_PRICE FLOAT COMMENT 'Unit price',

--Financial fields   
TOTAL_AMOUNT FLOAT COMMENT 'Total order amount',
PAYMENT_STATUS STRING COMMENT 'PAyment Status',

--Warehouse fields
 WAREHOUSE_ID STRING COMMENT 'Warehouse identifier',
 WAREHOUSE_LOCATION STRING COMMENT 'Warehouse location', 
 INVENTORY_LEVEL NUMBER COMMENT 'Current inventory level',

--Metadata
FILE_NAME STRING COMMENT 'Source file name',
FILE_ROW_NUMBER NUMBER COMMENT 'Row Number in source file',
INGESTED_AT TIMESTAMP_NTZ COMMENT 'When record was ingested',
TRANSFORMED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'When record was transformed'
)
COMMENT = 'Silver Stage 1 - flattened and cleaned supply chain orders';
  


-- Step -3 STREAM ON RAW_ORDERS

-- Capture new records from Bronze layer
create or replace stream silver_sch.raw_orders_stream
    on table fb_dev_db.bronze_sch.raw_orders
    append_only = true
    show_initial_rows = true
    comment = 'Stream on raw_orders - captures new data from processing';
    
--Verify stream created
show streams;

--STEP 4 - STORED PROCEDURE: SP_BRONZE_TO_SILVER 
-- Purpose :  Two responsibilities: 
--            1. Route bad records to DEAD_LETTER 
--            2. MERGE clean records into STG_ORDERS 
-- Why SP  :  Reusable, independently testable, clean separation 
--            from Task orchestration logic 
-- Called by: BRONZE_TO_SILVER_TASK (every 1 minute) 
-- Test by : CALL SILVER_SCH.SP_BRONZE_TO_SILVER()

create or replace procedure silver_sch.sp_bronze_to_silver()
    returns string
    language sql
    comment = 'SP - routes bad records to dead letter and merges clean records to stg_orders - will be called by task'
as
begin
    -- Part 0 - Buffer streaam into temp table
    -- Stream can only be consumed once per DML statement
    -- We materialise it here so both DEAD_LETTER and MERGE
    -- Can read from the same snapshot.
    create or replace temporary table silver_sch.stream_buffer as 
    select * from silver_sch.raw_orders_stream
    where metadata$action = 'INSERT';
    
    -- Part 1 - Route bad records to DEAD LETTER
    -- Only truly uncovererable records are rejected
    -- Validation is case-insesitive using UPPER(TRIM())
    -- Order_date: reject only when both conditions fail
    -- (AND logic) - ISO format passes TRY_TO_TIMESTAMP_NTZ
    -- DD-MM-YYYY matches LIKE pattern
    -- Order_status: UPPER(TRIM()) before IN check
    -- So "shipped"/"Shipped"/"SHIPPED" all pass
    INSERT INTO SILVER_SCH.DEAD_LETTER (
        RAW_DATA,
        ERROR_REASON,
        FILE_NAME,
        FILE_ROW_NUMBER
        )
       select 
          s.RAW_DATA,
          case
                ----- order level validations ------
             WHEN S.RAW_DATA:order_id::STRING IS NULL
                    THEN 'Missing order_id'
            WHEN TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NULL 
                    AND S.RAW_DATA:order_date::STRING NOT LIKE '__-__-____'
                    THEN 'Invalid or missing order_date' 
            WHEN UPPER(TRIM(S. RAW_DATA:order_status::STRING)) NOT IN ('PENDING', 'PROCESSING', 'SHIPPED','IN TRANSIT', 'DELIVERED', 'CANCELLED')
                THEN 'Invalid order_status: ' || COALESCE(S.RAW_DATA:order_status::STRING, 'NULL') 

               ----- Customer level validations-----
            WHEN S.RAW_DATA:customer.customer_id::STRING IS NULL 
                    THEN 'Missing customer_id'

               -----Supplier level------
            WHEN S.RAW_DATA:supplier.supplier_id::STRING IS NULL 
                    THEN 'Missing supplier_id'
            WHEN S.RAW_DATA:supplier.performance_score::FLOAT > 100 
                    THEN 'Invalid performance_score > 100: ' || S.RAW_DATA:supplier.performance_score::STRING 
            WHEN S.RAW_DATA:supplier.performance_score::FLOAT < 0 
                    THEN 'Invalid performance_score < 0: '  || S.RAW_DATA:supplier.performance_score::STRING
            WHEN S.RAW_DATA:supplier.lead_time_days::NUMBER < 0
                    THEN 'Invalid lead_time_days < 0:' || S.RAW_DATA:supplier.lead_time_days::STRING
                    
                -----Product level-------
            WHEN S.RAW_DATA:items[0].product_id::STRING IS NULL 
                    THEN 'Missing product_id'
            WHEN S.RAW_DATA:items[0].quantity::NUMBER IS NULL 
                    OR S.RAW_DATA:items[0].quantity::NUMBER <= 0 
                    THEN 'Invalid quantity: ' || COALESCE(S.RAW_DATA:items[0].quantity::STRING, 'NULL')
            WHEN S.RAW_DATA:items[0].unit_price::FLOAT IS NULL 
                    OR S.RAW_DATA:items[0].unit_price::FLOAT < 0    
                    THEN 'Invalid unit_price: ' || COALESCE(S.RAW_DATA:items[0].unit_price::STRING, 'NULL')

              -----Financial level-------
              WHEN S.RAW_DATA:financials.total_amount::FLOAT IS NULL
                    OR S.RAW_DATA:financials.total_amount::FLOAT < 0 
                    THEN 'Invalid total amount: ' || COALESCE(S.RAW_DATA:financials.total_amount::STRING, 'NULL')

              -----Warehouse level-----
              WHEN S.RAW_DATA:Warehouse.inventory_level::NUMBER < 0 
                    THEN 'Invalid inventory_level < 0: ' || S.RAW_DATA:warehouse.inventory_level::STRING

                 ELSE 'Unknown' 
                END AS ERROR_REASON, 
                S.FILE_NAME,
                S.FILE_ROW_NUMBER
             FROM SILVER_SCH.STREAM_BUFFER AS S
             WHERE
                    (
                      ---Order Level---
                  S.RAW_DATA:order_id::STRING IS NULL
                  or (
                        TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NULL 
                        AND S.RAW_DATA:order_date::STRING NOT LIKE '__-__-____'
                  )
                  OR UPPER(TRIM(S.RAW_DATA:order_status::STRING)) NOT IN (
                         'PENDING', 'PROCESSING', 'SHIPPED', 'IN TRANSIT', 'DELIVERED', 'CANCELLED'

                    )
                       ----Customer level----
                       OR S.RAW_DATA:customer.customer_id::STRING IS NULL
                       ---Supplier level----
                       OR S.RAW_DATA:supplier.supplier_id::STRING IS NULL 
                       OR S.RAW_DATA:supplier.performance_score::FLOAT > 100 
                       OR S.RAW_DATA:supplier.performance_score::FLOAT < 0 
                       OR S.RAW_DATA:supplier.lead_time_days::NUMBER < 0 
                       ----Product level----
                       OR S.RAW_DATA:items[0].product_id::STRING IS NULL
                       OR S.RAW_DATA:items[0].quantity::NUMBER IS NULL 
                       OR S.RAW_DATA:items[0].quantity::NUMBER <= 0 
                       OR S.RAW_DATA:items[0].unit_price::FLOAT IS NULL 
                       OR S.RAW_DATA:items[0].unit_price::FLOAT < 0 
                       ---- Financial level----
                       OR S.RAW_DATA:financials.total_amount::FLOAT IS NULL 
                       OR S.RAW_DATA:financials.total_amount::FLOAT < 0 
                       ---Warehouse level---
                       OR S.RAW_DATA:warehouse.inventory_level::NUMBER < 0
                    );
-- Part 2 - MERGE clean records into stg_orders
-- WHEN MATCHED  -> UPDATE changed field only
-- WHEN NOT MATCHED -> INSERT new order

    MERGE INTO SILVER_SCH.STG_ORDERS AS TGT 
    USING (
        SELECT
            ---order field----
            UPPER (TRIM(S.RAW_DATA:order_id::STRING)) AS ORDER_ID, 
            TRY_TO_TIMESTAMP_NTZ(
            CASE
              WHEN S.RAW_DATA:order_date::STRING LIKE '__-__-____'
                THEN TO_VARCHAR( 
                    TO_DATE(S.RAW_DATA:order_date::STRING, 'DD-MM-YYYY'), 'YYYY-MM-DD') || 'T00:00:00Z' 
            ELSE S.RAW_DATA:order_date::STRING
            END
            ) AS ORDER_DATE,
            UPPER(TRIM(S.RAW_DATA:order_status::STRING)) AS ORDER_STATUS,
            
            ----Customer fields----
            UPPER (TRIM(S.RAW_DATA:customer.customer_id::STRING)) AS CUSTOMER_ID,
            COALESCE(UPPER(TRIM(S.RAW_DATA:customer.customer_name::STRING)), 'UNKNOWN') AS CUSTOMER_NAME, 
            COALESCE (UPPER (TRIM(S.RAW_DATA:customer.region::STRING)), 'UNKNOWN') AS CUSTOMER_REGION,
            COALESCE (UPPER(TRIM(S.RAW_DATA:customer.segment::STRING)), 'UNKNOWN') AS CUSTOMER_SEGMENT,
            
           ---- Supplier fields----
            UPPER(TRIM(S.RAW_DATA:supplier.supplier_id::STRING)) AS SUPPLIER_ID, 
            COALESCE (UPPER(TRIM(S.RAW_DATA:supplier.supplier_name::STRING)), 'UNKNOWN') AS SUPPLIER_NAME, 
            COALESCE (UPPER(TRIM(S.RAW_DATA:supplier.country::STRING)), 'UNKNOWN') AS SUPPLIER_COUNTRY,                                                         COALESCE (S.RAW_DATA:supplier.lead_time_days::NUMBER, 0) AS LEAD_TIME_DAYS, 
            COALESCE(S.RAW_DATA:supplier.performance_score::FLOAT, 0) AS PERFORMANCE_SCORE,
            
            ----Shipment fields ----
            COALESCE (UPPER(TRIM(S.RAW_DATA:shipment.shipment_id::STRING)), 'UNKNOWN') AS SHIPMENT_ID, 
            COALESCE (UPPER(TRIM(S.RAW_DATA:shipment.carrier::STRING)), 'UNKNOWN') AS CARRIER,       
            TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:shipment.ship_date::STRING) AS SHIP_DATE,
            TRY_TO_DATE(S.RAW_DATA:shipment.estimated_delivery::STRING) AS ESTIMATED_DELIVERY, 
            GREATEST (COALESCE(S.RAW_DATA:shipment.delay_days::NUMBER, 0), 0) AS DELAY_DAYS,

            ----- Product fields (items[0] - first array element) -----
            UPPER(TRIM(S.RAW_DATA:items[0].product_id::STRING)) AS PRODUCT_ID,
            COALESCE (UPPER(TRIM(S.RAW_DATA:items[0].product_name::STRING)), 'UNKNOWN') AS PRODUCT_NAME,                                                        COALESCE (UPPER(TRIM(S.RAW_DATA:items[0].category::STRING)), 'UNKNOWN') AS CATEGORY,
            S.RAW_DATA:items[0].quantity::NUMBER AS QUANTITY,
            S.RAW_DATA:items[0].unit_price::FLOAT AS UNIT_PRICE,
            
             ---- Financial fields -----
             S.RAW_DATA:financials.total_amount::FLOAT AS TOTAL_AMOUNT,  
             COALESCE (UPPER(TRIM(S.RAW_DATA:financials.payment_status::STRING)), 'UNKNOWN') AS PAYMENT_STATUS,
             
              ----- Warehouse fields ----
              COALESCE (UPPER(TRIM(S.RAW_DATA:warehouse.warehouse_id::STRING)), 'UNKNOWN') AS WAREHOUSE_ID, 
              COALESCE (UPPER(TRIM(S.RAW_DATA:warehouse.warehouse_location::STRING)), 'UNKNOWN') AS WAREHOUSE_LOCATION, 
              COALESCE (S.RAW_DATA:warehouse.inventory_level::NUMBER, 0) AS INVENTORY_LEVEL,
              
               ----- Metadata -----
                S.FILE_NAME,
                S.FILE_ROW_NUMBER,
                S.INGESTED_AT
            FROM SILVER_SCH.STREAM_BUFFER S
            WHERE
                S.RAW_DATA:order_id::STRING IS NOT NULL
                AND(
                TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NOT NULL 
                OR S.RAW_DATA:order_date::STRING LIKE '__-__-____'
                )
                AND UPPER(TRIM(S.RAW_DATA:order_status::STRING)) IN (
                'PENDING', 'PROCESSING', 'SHIPPED',
                'IN TRANSIT', 'DELIVERED', 'CANCELLED'
                )
                AND S.RAW_DATA:customer.customer_id::STRING IS NOT NULL
                AND S.RAW_DATA:supplier.supplier_id::STRING IS NOT NULL 
                AND S. RAW_DATA: supplier.performance_score::FLOAT BETWEEN 0 AND 100
                AND (
                     S.RAW_DATA:supplier.lead_time_days::NUMBER IS NULL 
                     OR S.RAW_DATA:supplier.lead_time_days::NUMBER >= 0
                     )
                AND S. RAW_DATA:items[0].product_id::STRING IS NOT NULL 
                AND S.RAW_DATA:items[0].quantity::NUMBER IS NOT NULL 
                AND S.RAW_DATA:items[0].quantity::NUMBER > 0 
                AND S.RAW_DATA:items[0].unit_price::FLOAT IS NOT NULL
                AND S.RAW_DATA:items[0].unit_price::FLOAT >= 0 
                AND S.RAW_DATA:financials.total_amount::FLOAT IS NOT NULL 
                AND S.RAW_DATA:financials.total_amount::FLOAT >= 0 
                AND ( 
                    S.RAW_DATA:warehouse.inventory_level::NUMBER IS NULL
                    OR S.RAW_DATA:warehouse.inventory_level::NUMBER >= 0
                    ) 
                ) AS SRC 
                ON TGT.ORDER_ID = SRC.ORDER_ID 
                WHEN MATCHED THEN UPDATE SET
                        TGT.ORDER_STATUS = SRC.ORDER_STATUS,
                        TGT.CARRIER = SRC.CARRIER,
                        TGT.SHIP_DATE = SRC.SHIP_DATE,
                        TGT.ESTIMATED_DELIVERY = SRC.ESTIMATED_DELIVERY,
                        TGT.DELAY_DAYS = SRC.DELAY_DAYS,
                        TGT.INVENTORY_LEVEL = SRC.INVENTORY_LEVEL,
                        TGT.PAYMENT_STATUS = SRC.PAYMENT_STATUS,
                        TGT.CUSTOMER_NAME = SRC. CUSTOMER_NAME,
                        TGT.CUSTOMER_REGION = SRC.CUSTOMER_REGION,
                        TGT.SUPPLIER_NAME = SRC. SUPPLIER_NAME,
                        TGT.PERFORMANCE_SCORE = SRC.PERFORMANCE_SCORE,
                        TGT.WAREHOUSE_ID = SRC. WAREHOUSE_ID,
                        TGT.WAREHOUSE_LOCATION = SRC.WAREHOUSE_LOCATION,
                        TGT.TRANSFORMED_AT = CURRENT_TIMESTAMP () 
                WHEN NOT MATCHED THEN INSERT (
                    ORDER_ID, ORDER_DATE, ORDER_STATUS, 
                    CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_REGION, CUSTOMER_SEGMENT, 
                    SUPPLIER_ID, SUPPLIER_NAME, SUPPLIER_COUNTRY, LEAD_TIME_DAYS, PERFORMANCE_SCORE,
                    SHIPMENT_ID, CARRIER, SHIP_DATE, ESTIMATED_DELIVERY, DELAY_DAYS,
                    PRODUCT_ID, PRODUCT_NAME, CATEGORY, QUANTITY, UNIT_PRICE, 
                    TOTAL_AMOUNT, PAYMENT_STATUS, 
                    WAREHOUSE_ID, WAREHOUSE_LOCATION, INVENTORY_LEVEL, 
                    FILE_NAME, FILE_ROW_NUMBER, INGESTED_AT 
                    ) VALUES ( 
                        SRC.ORDER_ID, SRC.ORDER_DATE, SRC.ORDER_STATUS,
                        SRC.CUSTOMER_ID, SRC.CUSTOMER_NAME, SRC.CUSTOMER_REGION, SRC.CUSTOMER_SEGMENT,
                        SRC.SUPPLIER_ID, SRC.SUPPLIER_NAME, SRC.SUPPLIER_COUNTRY, SRC.LEAD_TIME_DAYS, SRC.PERFORMANCE_SCORE,
                        SRC.SHIPMENT_ID, SRC.CARRIER, SRC.SHIP_DATE, SRC.ESTIMATED_DELIVERY, SRC.DELAY_DAYS,
                        SRC.PRODUCT_ID, SRC.PRODUCT_NAME, SRC.CATEGORY, SRC.QUANTITY, SRC.UNIT_PRICE, 
                        SRC.TOTAL_AMOUNT, SRC.PAYMENT_STATUS, 
                        SRC.WAREHOUSE_ID, SRC.WAREHOUSE_LOCATION, SRC.INVENTORY_LEVEL, 
                        SRC.FILE_NAME, SRC.FILE_ROW_NUMBER, SRC.INGESTED_AT
                    );
                DROP TABLE IF EXISTS SILVER_SCH.STREAM_BUFFER;

                RETURN 'sp_bronze_to_silver completed successfully';
      END;    
      
-- Step 5 - TASK: bronze_to_silver_task
-- Purpose : Orchestrates sp_bronze_to_silver every 1 minute
-- Schedule : 1 MINUTE
-- WHEN : SYSTEM$STREAM_HAS_DATA - no data = no run = no cost
-- Note : TASK created SUSPENDED by default

create or replace task silver_sch.bronze_to_silver_task 
    warehouse = fb_pipeline_wh
    schedule = '1 minute'
    comment = 'Task - which calls sp_bronze_to_silver every minute when raw_orders_stream has data'
when system$stream_has_data('silver_sch.raw_orders_stream')
as 
    call silver_sch.sp_bronze_to_silver();

-- Step 6 - RESUME Task
alter task silver_sch.bronze_to_silver_task resume;

show tasks;

-- Step 7 - Test stored Procedure directly
 call silver_sch.sp_bronze_to_silver();

-- Step 8 - Verify

select system$stream_has_data('silver_sch.raw_orders_stream');

select count(*) as stg_orders_count from silver_sch.stg_orders
union all
select count(*) as stg_orders_count from silver_sch.dead_letter;

select * from silver_sch.dead_letter;

select * from silver_sch.stg_orders;