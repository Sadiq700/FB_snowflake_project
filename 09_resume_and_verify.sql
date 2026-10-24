use role sysadmin;
use warehouse fb_pipeline_wh;

-- 1 Resume Pipe
ALTER pipe fb_prod_db.bronze_sch.supply_chain_pipe_prod set pipe_execution_paused = false;

select system$pipe_status('fb_prod_db.bronze_sch.supply_chain_pipe_prod');

-- 2. Resume Tasks
ALTER task fb_prod_db.silver_sch.bronze_to_silver_task resume;
ALTER task fb_prod_db.silver_sch.silver_to_star_task resume;

show tasks in database fb_prod_db;

-- 3. resume alerts
use role accountadmin;
alter alert fb_prod_db.bronze_sch.pipeline_health_alert resume;
show alerts in database fb_prod_db;

--4. Recreate streams (invalidated by schema clone / table replace)
use role sysadmin;
create or replace stream fb_prod_db.silver_sch.raw_orders_stream
    on table fb_prod_db.bronze_sch.raw_orders
    append_only = true
    comment = 'Stream on raw_orders - captures new data for silver processing';


-- 5. Resume Dynamic tables

alter dynamic table fb_prod_db.gold_sch.agg_base resume;
alter dynamic table fb_prod_db.gold_sch.agg_inventory_turnover resume;
alter dynamic table fb_prod_db.gold_sch.agg_order_fulfillment resume;
alter dynamic table fb_prod_db.gold_sch.agg_shipment_delays resume;
alter dynamic table fb_prod_db.gold_sch.supplier_performance resume;

-- Start ingesting data

-- 6. Verify Row counts
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

-- 7. Verify Secure Views
select * from fb_prod_db.serving_sch.vw_inventory_level_turnover;
select * from fb_prod_db.serving_sch.vw_orders_fulfillment;
select * from fb_prod_db.serving_sch.vw_shipment_delays;
select * from fb_prod_db.serving_sch.vw_supplier_performance;

list @bronze_sch.adls_raw_stage_prod;

show file formats in schema
fb_prod_db.bronze_sch;
 show streams in schema fb_prod_db.silver_sch;
  select get_ddl( 
    'procedure',
    'fb_prod_db.silver_sch.silver_to_star()'
  );