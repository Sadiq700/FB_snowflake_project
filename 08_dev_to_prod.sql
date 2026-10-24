-- =====================================================================
-- Snowflake Supply Chain Project
-- Script : 08_promotion/dev_to_prod.sql
-- Purpose : Promote DEV environment to PROD 
--            using Snowflake zero-copy cloning, data sharing, managed account 
-- Run as : ACCOUNTADMIN > then SYSADMIN 
-- Note : Run each step individually, not all at once

-- WHY ZERO-COPY CLONING?
-- -----------------------
-- Traditional promotion: recreate everything from scratch 
--     -> Time consuming, error prone, data loss risk
--
-- Snowflake zero-copy clone:
--      -> Instant - no data copving 
--      -> No extra storage cost at clone time 
--      -> Exact replica of DEV including all data
--      -> Safe - DEV remains untouched
-- =====================================================================
use role accountadmin;

-- =====================================================================
-- Step 1 - Colne All schemas from DEVto PROD
-- Pourpose : Zero-copy clone each schema
--             All tables, views, sequences cloned instantly
-- =====================================================================

-- Clone Bronze schema
create or replace schema fb_prod_db.bronze_sch
    clone fb_dev_db.bronze_sch;

-- Clone Silver schema
create or replace schema fb_prod_db.silver_sch
    clone fb_dev_db.silver_sch;

-- Clone Gold schema
create or replace schema fb_prod_db.gold_sch
    clone fb_dev_db.gold_sch;
    
-- Clone Serving schema
create or replace schema fb_prod_db.serving_sch
    clone fb_dev_db.serving_sch;

-- Verify all schemas cloned
show schemas in database fb_prod_db;

-- Grant ownership to sysadmin
grant ownership on schema fb_prod_db.bronze_sch to role sysadmin copy current grants;
grant ownership on schema fb_prod_db.silver_sch to role sysadmin copy current grants;
grant ownership on schema fb_prod_db.gold_sch to role sysadmin copy current grants;
grant ownership on schema fb_prod_db.serving_sch to role sysadmin copy current grants;

-- =====================================================================
-- Step 2 - Verify row counts match dev
-- Purpose : Confirm all data cloned correctly
--           DEV and PROD counts should match exactly
-- =====================================================================
use role sysadmin;

--Bronze 
select 'DEV' as env, count(*) as raw_orders from fb_dev_db.bronze_sch.raw_orders 
union all 
select 'PROD' as env, count(*) as raw_orders from fb_prod_db.bronze_sch.raw_orders;

-- Silver
select 'DEV' as env, count(*) as stg_orders from fb_dev_db.silver_sch.stg_orders 
union all 
select 'PROD' as env, count(*) as stg_orders from fb_prod_db.silver_sch.stg_orders; 

select 'DEV' as env, count(*) as fact_orders from fb_dev_db.silver_sch.fact_orders
union all 
select 'PROD' as env, count(*) as fact_orders from fb_prod_db.silver_sch.fact_orders;

--Gold 
select 'DEV' as env, count(*) as agg_base from fb_dev_db.gold_sch.agg_base
union all
select 'PROD' as env, count(*) as agg_base from fb_prod_db.gold_sch.agg_base;

-- =====================================================================
-- STEP 3 - RECREATE STORAGE INTEGRATION FOR PROD 
-- Purpose : Storage integration points to PROD container
--           Alreadv includes PROD URL in allowed locations
--           from bronze.sql - no change needed
-- =====================================================================

-- Verify storage integration cover PROD container
desc integration fb_adls_integration;

-- =====================================================================
-- Step 4 - Recreate SNOWPIPE for PROD
-- Purpose : Snowpipe cannot be cloned
--           must recreate pointing to PROD stage
--           needs new Azure Event Grid subscription
-- =====================================================================
use role sysadmin;
use database fb_prod_db;
use schema bronze_sch;
use warehouse fb_pipeline_wh;


-- Create PROD external stage 
create or replace stage  bronze_sch.adls_raw_stage_prod
    url = 'azure://fbprojectadls.blob.core.windows.net/supply-chain-raw-prod/'
    storage_integration = fb_adls_integration
    file_format = bronze_sch.json_file_format
    comment = 'External stage - ADLS Gen2 PROD container';

list @bronze_sch.adls_raw_stage_prod;

show tables in database fb_prod_db;

truncate table if exists fb_prod_db.bronze_sch.raw_orders;
truncate table if exists fb_prod_db.silver_sch.stg_orders;
truncate table if exists FB_PROD_DB.SILVER_SCH.FACT_ORDERS;
truncate table if exists fb_prod_db.silver_sch.dim_customer;
truncate table if exists fb_prod_db.silver_sch.dim_product;
truncate table if exists fb_prod_db.silver_sch.dim_shipment;
truncate table if exists fb_prod_db.silver_sch.dim_supplier;
truncate table if exists fb_prod_db.silver_sch.dim_warehouse;
truncate table if exists fb_prod_db.silver_sch.dead_letter;

select * from FB_PROD_DB.SILVER_SCH.FACT_ORDERS;

alter dynamic table fb_prod_db.gold_sch.agg_base refresh;
alter dynamic table fb_prod_db.gold_sch.agg_inventory_turnover refresh;
alter dynamic table fb_prod_db.gold_sch.agg_order_fulfillment refresh;
alter dynamic table fb_prod_db.gold_sch.agg_shipment_delays refresh;
alter dynamic table fb_prod_db.gold_sch.supplier_performance refresh;

select * from fb_prod_db.gold_sch.agg_order_fulfillment;

-- Create PROD Snowpipe
create or replace pipe bronze_sch.supply_chain_pipe_prod
    auto_ingest = true
    integration = fb_azure_notifications_int
    comment = 'Snowpipe - auto ingest json file from ADLS Gen2'
as
    copy into fb_prod_db.bronze_sch.raw_orders (
        raw_data,
        ingested_at,
        file_name,
        file_row_number
    )
from (
    select $1,
            current_timestamp(),
            metadata$filename,
            metadata$file_row_number
    from @fb_prod_db.bronze_sch.adls_raw_stage_prod
) file_format = (format_name = 'bronze_sch.json_file_format');

-- drop pipe FB_PROD_DB.BRONZE_SCH.SUPPLY_CHAIN_PIPE;

select system$pipe_status('fb_prod_db.bronze_sch.supply_chain_pipe_prod');

select get_ddl(
    'pipe',
    'fb_prod_db.bronze_sch.supply_chain_pipe_prod'
);
alter pipe fb_prod_db.bronze_sch.supply_chain_pipe_prod refresh;
-- =====================================================================
-- Step 5 - Recreate STREAMS for PROD
-- Purpose : Cloned Stream have no accessible unconsumed records
--           Recreate to start fresh on PROD tables
-- =====================================================================
-- Stream on PROD RAW_ORDERS
create or replace stream fb_prod_db.silver_sch.raw_orders_stream
     on table fb_prod_db.bronze_sch.raw_orders
     append_only = true
     show_initial_rows = true
     comment = 'Stream on raw_orders - captures new data from processing';

-- Stream on PROD RAW_ORDERS
create or replace stream fb_prod_db.silver_sch.stg_orders_stream
    on table fb_prod_db.silver_sch.stg_orders
    show_initial_rows = true
    comment = 'stream on stg_orders';

show streams in database fb_prod_db;
-- =====================================================================
-- Step 5B - Recreate stored procedures for PROD
-- Purpose : SPs may be lost if schema id=s re-cloned
--           must exist before task can call them 
-- =====================================================================
create or replace procedure fb_prod_db.silver_sch.sp_bronze_to_silver()
    returns string
    language sql
    comment = 'SP - routes bad records to dead letter and merges clean records to stg_orders - will be called by task'
as
begin
    -- Part 0 - Buffer streaam into temp table
    -- Stream can only be consumed once per DML statement
    -- We materialise it here so both DEAD_LETTER and MERGE
    -- Can read from the same snapshot.
    create or replace temporary table fb_prod_db.silver_sch.stream_buffer as 
    select * from fb_prod_db.silver_sch.raw_orders_stream
    where metadata$action = 'INSERT';
    
    -- Part 1 - Route bad records to DEAD LETTER
    -- Only truly uncovererable records are rejected
    -- Validation is case-insesitive using UPPER(TRIM())
    -- Order_date: reject only when both conditions fail
    -- (AND logic) - ISO format passes TRY_TO_TIMESTAMP_NTZ
    -- DD-MM-YYYY matches LIKE pattern
    -- Order_status: UPPER(TRIM()) before IN check
    -- So "shipped"/"Shipped"/"SHIPPED" all pass
    INSERT INTO fb_prod_db.SILVER_SCH.DEAD_LETTER (
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
              WHEN S.RAW_DATA:warehouse.inventory_level::NUMBER < 0 
                    THEN 'Invalid inventory_level < 0: ' || S.RAW_DATA:warehouse.inventory_level::STRING

                 ELSE 'Unknown' 
                END AS ERROR_REASON, 
                S.FILE_NAME,
                S.FILE_ROW_NUMBER
             FROM fb_prod_db.SILVER_SCH.STREAM_BUFFER AS S
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

    MERGE INTO fb_prod_db.SILVER_SCH.STG_ORDERS AS TGT 
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
            COALESCE (UPPER(TRIM(S.RAW_DATA:items[0].product_name::STRING)), 'UNKNOWN') AS PRODUCT_NAME, 
            COALESCE (UPPER(TRIM(S.RAW_DATA:items[0].category::STRING)), 'UNKNOWN') AS CATEGORY,
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
            FROM fb_prod_db.SILVER_SCH.STREAM_BUFFER S
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
                DROP TABLE IF EXISTS fb_prod_db.SILVER_SCH.STREAM_BUFFER;

                RETURN 'sp_bronze_to_silver completed successfully';
      END;    

----------------------------------------------------------------------------------
create or replace procedure fb_prod_db.silver_sch.silver_to_star()
    returns string
    language sql
as
begin
    ---- Part 1 - TEMP TABLE
    create or replace temporary table fb_prod_db.silver_sch.temp_stream_data as
    select * from fb_prod_db.silver_sch.stg_orders_stream
    where metadata$action = 'INSERT';

    --- Part 2 - Merge into dim_customer (SCD1 - upsert)
    merge into fb_prod_db.silver_sch.dim_customer as tgt 
    using (
        select
            upper(customer_id) as customer_id,
            upper(customer_name) as customer_name,
            upper(customer_region) as customer_region,
            upper(customer_segment) as customer_segment
        from fb_prod_db.silver_sch.temp_stream_data
        qualify row_number() over (partition by upper(customer_id) order by order_date desc) =1
    ) as src 
    on tgt.customer_id = src.customer_id
    when matched then update set 
        tgt.customer_name = src.customer_name,
        tgt.customer_region = src.customer_region,
        tgt.customer_segment = src.customer_segment,
        tgt.is_active        = true,
        tgt.updated_at        = current_timestamp()
    when not  matched then insert (
        customer_id, customer_name, customer_region, customer_segment
    ) values (
        src.customer_id, src.customer_name, src.customer_region, src.customer_segment
    );

 --- Part 3 - Merge into dim_product (SCD1 - upsert)
merge into fb_prod_db.silver_sch.dim_product as tgt 
using (
    select
        upper(product_id) as product_id,
        upper(product_name) as product_name,
        upper(category) as category,
        unit_price
    from fb_prod_db.silver_sch.temp_stream_data
    qualify row_number() over(partition by upper(product_id) order by order_id desc) = 1
) as src 
on tgt.product_id = src.product_id
when matched then update set 
    tgt.product_name = src.product_name,
    tgt.category = src.category,
    tgt.unit_price = src.unit_price,
    tgt.is_active = true,
    tgt.updated_at = current_timestamp()
when not matched then insert(
product_id, product_name, category, unit_price
) values (
    src.product_id, src.product_name, src.category, src.unit_price
);
    
 --- Part 4 - Merge into dim_supplier (SCD1 - upsert)
 merge into fb_prod_db.silver_sch.dim_supplier as tgt
using(
select
    upper (supplier_id) as  supplier_id,
    upper (supplier_name) as supplier_name,
    upper (supplier_country) as supplier_country,
    lead_time_days,
    performance_score
from fb_prod_db.silver_sch.temp_stream_data 
qualify row_number() over (partition by upper (supplier_id) order by order_date desc) = 1
) as src
on tgt.supplier_id = src.supplier_id
when matched then update set
    tgt.supplier_name = src.supplier_name,
    tgt.supplier_country = src.supplier_country,
    tgt.lead_time_days = src.lead_time_days,
    tgt.performance_score = src.performance_score,
    tgt.is_active = true,
     tgt.updated_at =  current_timestamp()
when not matched then insert ( 
    supplier_id, supplier_name, supplier_country, lead_time_days, performance_score
) values (
    src.supplier_id, src.supplier_name, src.supplier_country, src.lead_time_days, src.performance_score
    );
    
 --- Part 5 - Merge into dim_warehouse (SCD1 - upsert)
 merge into fb_prod_db.silver_sch.dim_warehouse as tgt
 using (
    select 
        upper (warehouse_id)       as warehouse_id,
        upper (warehouse_location) as warehouse_location
     from fb_prod_db.silver_sch.temp_stream_data 
     qualify row_number() over (partition by upper (warehouse_id) order by order_date desc) = 1
    ) as src 
    on tgt.warehouse_id = src.warehouse_id 
    when matched then update set 
            tgt.warehouse_location = src.warehouse_location, 
            tgt.is_active = true, 
            tgt.updated_at = current_timestamp() 
    when not matched then insert ( 
         warehouse_id, warehouse_location 
    ) values ( 
        src.warehouse_id, src.warehouse_location
);
 --- Part 6 - Merge into dim_shipment (SCD1 - upsert)
 merge into fb_prod_db.silver_sch.dim_shipment as tgt 
 using (
    select 
        upper (shipment_id) as shipment_id, 
        upper (carrier) as carrier, 
        ship_date, 
        estimated_delivery 
     from fb_prod_db.silver_sch.temp_stream_data 
     where upper (shipment_id) != 'UNKNOWN'
     qualify row_number() over (partition by upper (shipment_id) order by order_date desc) = 1
 )as src
  on tgt.shipment_id = src.shipment_id 
 when matched then update set 
     tgt.carrier  = src.carrier, 
     tgt.ship_date = src.ship_date, 
     tgt.estimated_delivery = src.estimated_delivery,
     tgt.is_active  = true, 
     tgt.updated_at =  current_timestamp() 
 when not matched then insert (
    shipment_id, carrier, ship_date, estimated_delivery 
 ) values (
    src.shipment_id, src.carrier, src.ship_date, src.estimated_delivery 
 );

     --- Part 7 - Merge into fact_orders 
merge into fb_prod_db.silver_sch.fact_orders as tgt
using (
    select
    upper(s.order_id) as order_id,
    dc.customer_sk, 
    dp.product_sk,
    ds.supplier_sk,
    dw.warehouse_sk,
    coalesce(dsh.shipment_sk, -1) as shipment_sk,
    s.order_date,
    upper (s.order_status) as order_status,
    upper (s.payment_status) as payment_status,
    s.quantity, 
    s.unit_price, 
    s.total_amount, 
    s.delay_days, 
    s.inventory_level,
    s.ingested_at
from fb_prod_db.silver_sch.temp_stream_data s
    inner join silver_sch.dim_customer dc on dc.customer_id = upper(s.customer_id)
    inner join silver_sch.dim_product dp on dp.product_id = upper(s.product_id)
    inner join silver_sch.dim_supplier ds on ds.supplier_id = upper(s.supplier_id)
    inner join silver_sch.dim_warehouse dw on dw.warehouse_id = upper(s.warehouse_id) 
    left join silver_sch.dim_shipment dsh on dsh.shipment_id = upper(s.shipment_id)
    qualify row_number() over (partition by upper(s.order_id) order by s.order_date desc) = 1
) as src
on tgt.order_id = src.order_id 
when matched then update set 
    tgt.customer_sk   = src.customer_sk,
    tgt.product_sk     = src.product_sk,
    tgt.supplier_sk    = src.supplier_sk,
    tgt.warehouse_sk   = src.warehouse_sk,
    tgt.shipment_sk    = src.shipment_sk,
    tgt.order_date = src.order_date, 
    tgt.order_status = src.order_status, 
    tgt.payment_status = src.payment_status, 
    tgt.quantity = src.quantity, 
    tgt.unit_price = src.unit_price, 
    tgt.total_amount = src.total_amount, 
    tgt.delay_days = src.delay_days, 
    tgt.inventory_level = src.inventory_level, 
    tgt.ingested_at = src.ingested_at,
    tgt.updated_at = current_timestamp()
when not matched then insert ( 
    order_id, customer_sk, product_sk, supplier_sk, warehouse_sk, shipment_sk, 
    order_date, order_status, payment_status, quantity, unit_price, total_amount, 
    delay_days, inventory_level, ingested_at
 ) values(
        src.order_id, src.customer_sk, src.product_sk, src.supplier_sk, src.warehouse_sk, src.shipment_sk, 
        src.order_date, src.order_status, src.payment_status, src.quantity, src.unit_price, src.total_amount,
        src.delay_days, src.inventory_level, src.ingested_at
       
    );

    --cleanup temp table - no longer needed after all MERGEs
    drop table if exists fb_prod_db.silver_sch.temp_stream_data;
    
    return 'sp_silver_to_star complete successfully';
end;

-- ===================================================================
-- Step 6 - Recreate TASK for PROD
-- Purpose : Cloned task are suspended by default
--           Recreate to point stored procedure
-- ===================================================================
-- Bronze to Silver task
create or replace task fb_prod_db.silver_sch.bronze_to_silver_task 
    warehouse = fb_pipeline_wh
    schedule = '1 minute'
    comment = 'Task - which calls sp_bronze_to_silver every minute when raw_orders_stream has data'
when system$stream_has_data('fb_prod_db.silver_sch.raw_orders_stream')
as 
    call silver_sch.sp_bronze_to_silver();

-- Silver to star task 
create or replace task fb_prod_db.silver_sch.silver_to_star_task
    warehouse = fb_pipeline_wh
    schedule = '1 minute'
    when system$stream_has_data('fb_prod_db.silver_sch.stg_orders_stream')
as
    call silver_sch.silver_to_star();

-- Verify
show tasks in database fb_prod_db;

--select get_ddl('dynamic_table''fb_prod_db.gold_sch.agg_base');
-- ===================================================================
-- Step 7 -  Recreate ALERTs for PROD
-- Purpose : Cloned alerts are suspended by default
--           Recreate to point to PROD database
-- ===================================================================
use role accountadmin;
create or replace alert FB_PROD_DB.bronze_sch.pipeline_health_alert
    warehouse = fb_pipeline_wh
    schedule = '5 minute'
    if(exists(
        ------------- Bronze - Snowpipe Failure ----------
        select 1 
        from table(information_schema.copy_history(
                table_name = 'RAW_ORDERS',
                start_time = dateadd(hour, -1, current_timestamp())
        ))
        where status = 'Load Failed'

        union all
        ------------- Silver - 2 task Failure ----------
        select 1 from table (information_schema.task_history(
            scheduled_time_range_start = dateadd(hour, -1, current_timestamp())
        ))
        where state = 'FAILED'
        and database_name = 'FB_PROD_DB'

        union all
        ------------- Gold - DT's Failure ----------
        select 1 from table(information_schema.dynamic_table_refresh_history())
        where schema_name = 'GOLD_SCH'
        and database = 'FB_PROD_DB'
        and state = 'FAILED'
        and refresh_start_time > dateadd(minute,-5,current_timestamp())
        
    ))
    then call system$send_email(
        'email_notification_int',
        'sadiqtech83@gmail.com',
        'Pipeline Alert! - Flowbridge Project!',
        'Something went wwrong check bronze/Silver/Gold layers for failure. Login to snowsight -> monitoring ->
        Task history / monitor history'
    );

ALTER alert FB_PROD_DB.bronze_sch.pipeline_health_alert resume;

-- ===============================================================
-- Step 8 - Switch data sharing to PROD
-- Purpose : Update share to use PROD serving layer
-- ===============================================================
use role accountadmin;

create or replace share fb_prod_share
    comment = ' Folwbridge data for serving views';

-- Add PROD grant to share 
grant usage on database fb_prod_db
    to share fb_prod_share;

grant usage on schema  fb_prod_db.serving_sch
    to share fb_prod_share;

grant select on view fb_prod_db.serving_sch.vw_orders_fulfillment
    to share fb_prod_share;

grant select on view fb_prod_db.serving_sch.vw_supplier_performance
    to share fb_prod_share;;

grant select on view fb_prod_db.serving_sch.vw_inventory_level_turnover
    to share fb_prod_share;

grant select on view fb_prod_db.serving_sch.vw_shipment_delays
    to share fb_prod_share;

-- Verify Share
desc share fb_prod_share;


-- Verify grants
show grants on share fb_prod_share;
show grants to share fb_prod_share;

-- =========================================================
-- STEP 9 - READFR ACCOUNT FOR LOGTSTTCS PARTNER 
-- Purpose : Creates a FREE managed Snowflake account for partnen
--           Partner gets login credentials
--           They can ONLY query what you shared (4 PROD views)
--           You manage and pay for the account 
 -- Why :    Logistics partner doesn't have Snowflake 
--            Reader account = simplest way to give access 
-- Control : You decide what they see (Secure Views only) 
--           You can revoke access anytime
-- =======================================================================

-- Run these in your session BEFORE executing the CREATE below (do NOT save passwords in files):
set reader_admin_name = 'flowbridge_admin';
set reader_admin_password = 'Project@123456789';

create managed account if not exists fb_partner_account
    admin_name = $reader_admin_name
    admin_password = $reader_admin_password
    type = reader
    comment = 'Reader account for Flowbridge Logistics Partners - access to PROD KPI only';


--Get reader account details 
show managed accounts;
--Copy the LOCATOR value - share it with partner


-- Add reader account to share - this is what gives them access
-- A After copying locator from SHOW MANAGED ACCOUNTS above: 
-- SET READER_LOCATOR = '<paste locator here>';

alter share fb_prod_share  
    add accounts = "UR19439";

-- Step 1: Call procedures to populate tables
grant ownership on stream fb_prod_db.silver_sch.raw_orders_stream to role sysadmin copy current grants;
grant ownership on stream fb_prod_db.silver_sch.stg_orders_stream to role sysadmin copy current grants;

use role sysadmin;

call fb_prod_db.silver_sch.sp_bronze_to_silver();
call fb_prod_db.silver_sch.silver_to_star();

-- Step 2: Resume tasks for ongoing processing
ALTER task fb_prod_db.silver_sch.bronze_to_silver_task resume;
ALTER task fb_prod_db.silver_sch.silver_to_star_task resume;

alter dynamic table fb_prod_db.gold_sch.agg_base resume;
alter dynamic table fb_prod_db.gold_sch.agg_inventory_turnover resume;
alter dynamic table fb_prod_db.gold_sch.agg_order_fulfillment resume;
alter dynamic table fb_prod_db.gold_sch.agg_shipment_delays resume;
alter dynamic table fb_prod_db.gold_sch.supplier_performance resume;

-- Step 3: Verify row counts
select 'raw_orders' as tbl, count(*) as CNT from fb_prod_db.bronze_sch.raw_orders
union all select 'stg_orders', count(*) from fb_prod_db.silver_sch.stg_orders
union all  select 'dim_customer', count(*) from fb_prod_db.silver_sch.dim_customer
union all select  'dim_product', count(*) from fb_prod_db.silver_sch.dim_product
union all  select 'dim_shipment', count(*) from fb_prod_db.silver_sch.dim_shipment
union all  select 'dim_supplier', count(*) from fb_prod_db.silver_sch.dim_supplier
union all  select  'dim_warehouse', count(*) from fb_prod_db.silver_sch.dim_warehouse
union all  select 'fact_orders', count(*) from fb_prod_db.silver_sch.fact_orders
union all  select 'dead_letter', count(*) from fb_prod_db.silver_sch.dead_letter
union all  select 'agg_base', count(*) from fb_prod_db.gold_sch.agg_base
union all  select 'agg_inventory_turnover', count(*) from fb_prod_db.gold_sch.agg_inventory_turnover
union all  select  'agg_order_fulfillment', count(*) from fb_prod_db.gold_sch.agg_order_fulfillment
union all  select 'agg_shipment_delays', count(*) from fb_prod_db.gold_sch.agg_shipment_delays
union all  select 'supplier_performance', count(*) from fb_prod_db.gold_sch.supplier_performance;


show streams in schema fb_prod_db.silver_sch;

describe stream fb_prod_db.silver_sch.raw_orders_stream;

select get_ddl(
    'procedure',
    'fb_prod_db.silver_sch.silver_to_star()'
    );

-- =======================================================================

-- STEP 10 - FINAL VERIFICATION 
-- Purpose : Confirm PROD is fully operational
-- =======================================================================

--Check all table counts in PROD
/*
show resource monitors;

show resource monitors like 'fb_pipeline_rm';

use role accountadmin;

alter resource monitor fb_pipeline_rm 
    set credit_quota = 80;

alter warehouse fb_pipeline_wh resume;
*/

select count(*) from serving_sch.vw_inventory_level_turnover;

alter warehouse fb_pipeline_wh 
    set auto_suspend = 60
    auto_resume = false;
alter warehouse fb_pipeline_wh resume;
