-- ============================================================
-- Snowflake Supply Chain Project 
-- Script : 05 gold/gold.sql 
-- Purpose : Gold layer - Dynamic Tables 
--       AGG BASE (base laver - joins silver dims + fact)
--          AGG ORDER FULFILLMENT (downstream 
--          AGG SUPPLIER PERFORMANCE (downstream 
--          AGG_INVENTORY _TURNOVER (downstream 
--          AGG SHIPMENT _DELAYS (downstream) 
-- Run as : SYSADMIN 
-- Note : Run each step individuallv, not all at once 
-- Dynamic Table pattern: 
-- AGG_BASE > LAG - '1 MINUTE' (queries silver)
-- All others -> LAG = DOWNSTREAM (refresh when base refreshes)
-- Benefit > Silver queried once, all KPIs consistent
-- ============================================================
use role sysadmin;
use warehouse fb_pipeline_wh;
use database fb_dev_db;
use schema gold_sch;

--STEP 1 - AGG BASE (Base Dynamic Table)

--Purpose : Joins FACT ORDERS with all 5 dimensions 
--          Single source of truth for all Gold KPIs 
--          All downstream tables query this - not Silver
-- LAG : 1 MINUTE - controls refresh cadence for all
--         downstream tables automatically
-- Why base : Silver layer queried only ONCE per refresh 
--             All downstream KPIs use same consistent snapshot
--             Cost efficient - no duplicate Silver queries 
--Join : LEFT JOIN DIM_SHIPMENT - order may not be shipped
-- ============================================================
create or replace dynamic table gold_sch.agg_base
    lag = '1 minute'
    warehouse = fb_pipeline_wh
    comment = 'Base dynamic table'
as
select
    ------order fields--------
        f.order_id,
        f.order_date,
        f.order_status,
        f.payment_status,
    -----Measures -------------
        f.quantity,
        f.unit_price,
        f.total_amount,
        f.delay_days,
        f.inventory_level,
    -----Pipeline metadata------
        f.ingested_at,
    ----Customer fields----------
        c.customer_id,
        c.customer_name,
       c.customer_region,
       c.customer_segment,
    ----Product fields-------
        p.product_id,
        p.product_name,
        p.category,
    -----Supplier fields------
        sp.supplier_id,
        sp.supplier_name,
        sp.supplier_country,
        sp.lead_time_days,
        sp.performance_score,
    ------Warehouse fields------
        w.warehouse_id,
       w.warehouse_location,
    ------Shipment fields------
    ------Left join --Null if order not yet shipped
        sh.shipment_id,
        sh.carrier,
        sh.ship_date,
        sh.estimated_delivery
    from silver_sch.fact_orders as f 
    join  silver_sch.dim_customer   c on f.customer_sk = c.customer_sk
    join  silver_sch.dim_product   p on f.product_sk = p.product_sk
    join  silver_sch.dim_supplier sp on f.supplier_sk = sp.supplier_sk
    join  silver_sch.dim_warehouse w on  f.warehouse_sk = w.warehouse_sk
    left join  silver_sch.dim_shipment sh on f.shipment_sk = sh.shipment_sk;

    select * from gold_sch.agg_base;
-- ============================================================

-- STEP 2 - AGG ORDER FULFILLMENT (Downstream) 
-- Purpose : Order fulfillment KPIs by customer region 
-- Source : AGG_BASE (not Silver directly)
-- LAG : DOWNSTREAM - refreshes when AGG BASE refreshes
-- KPIs : 
--      total_orders        - total orders per region 
--      delivered_orders    - orders with status delivered
--      pending_orders      - orders with status pending/processing
--      cancelled_orders    - orders with status cancelled
--      fulfullment_rate    - delivered / total * 100
--      total_revenue       -  sum of total_amount
--      avg_order_value     -   average order amount 
-- ============================================================
create or replace dynamic table gold_sch.agg_order_fulfillment
    lag = downstream
    warehouse = fb_pipeline_wh
    comment = 'Order fulfillment KPIs by customer region, downstream from AGG_BASE'
as
select
    customer_region,
    customer_segment,
    --order counts
    count(order_id) as total_orders,
    count(case when order_status = 'DELIVERED' then 1 end) as delivered_orders,
    count(case when order_status in ('PENDING','PROCESSING') then 1 end)  as pending_orders,
    count(case when order_status = 'CANCELLED' then 1 end) as cancelled_orders,
    count(case when order_status = 'IN TRANSIT' then 1 end) as in_transit_orders,
    --- Fuldillment rate - delivered / total * 100
    round(count (case when order_status = 'DELIVERED' then 1 end) / nullif(count(order_id),0) * 100,2) as fulfillment_rate_pct,
    --- Revenue KPIs
    round(sum(total_amount), 2) as total_revenue,
    round(avg(total_amount), 2) as avg_order_value,
    --Payment breakdown
    count(case when payment_status = 'PAID' then 1 end) as paid_orders,
    count(case when payment_status = 'PENDING' then 1 end) as payment_pending_orders,
    count(case when payment_status = 'OVERDUE' then 1 end) as overdue_orders
from gold_sch.agg_base
group by customer_region,
        customer_segment;
select * from gold_sch.agg_order_fulfillment;
-- ============================================================
-- STEP 3 - AGG SUPPLIER PERFORMANCE (Downstream)
-- Purpose  : Supplier performance 
-- Source  : AGG_BASE (not Silver directly)
-- LAG  :   DOWNSTREAM - refreshes when AGG BASE refreshes 
-- KPIs  :
--     total_orders             - orders per supplier
--     avg_per formance_score   - average score
--      total _revenue          - - revenue generatea
--      on_time_orders          - orders with delay_days = 0
--      delayed_orders          - orders with delay_days > 0
--      on_time_rate            - on time / total * 100
--      avg_delay_days          - average delay when delayed
-- ============================================================
create or replace dynamic table gold_sch.supplier_performance
    lag = downstream
    warehouse = fb_pipeline_wh
    comment = ' Supplier performance KPIs. Downstream from AGG_BASE'
as
select
    supplier_id,
    supplier_name,
    supplier_country,
    --order volume
    count(order_id) as total_orders,
    -- Performance metrics
    round(avg(performance_score), 2) as avg_performance_score,
    round(avg(lead_time_days), 2) as avg_lead_time_days,
    --Revenue
    round(sum(total_amount), 2) as total_revenue,
    round(avg(total_amount), 2) as avg_order_value,
    --Delay metrics
    count(case when delay_days = 0 then 1 end) as on_time_orders,
    count(case when delay_days > 0 then 1 end) as delayed_orders,
    --On time rate 
    round(count(case when delay_days = 0 then 1 end)/ nullif(count(order_id), 0) *100, 2) as on_time_rate_pct,
    -- Average delay when delayed
    round(avg(case when delay_days > 0 then delay_days end), 2) as avg_delay_days
from gold_sch.agg_base
group by 
    supplier_id,
    supplier_name,
    supplier_country;
select * from  gold_sch.supplier_performance;


-- ============================================================
-- - STEP 4 - AGG_INVENTORY_TURNOVER (Downstream) 
-- Purpose : Inventory KPIs by warehouse and product
-- Source  : AGG BASE (not Silver directly)
-- LAG     : DOWNSTREAM - refreshes when AGG_BASE refreshes
-- KPIs    :
--  total_orders        - orders processed per warehouse
--  total_quantity      - total unit ordered
--  avg_inventory_level - average inventory snapshot
-- min_inventory_level  -  lowest inventory recorded
-- max_inventory_level  -   highest inventory recorded
-- total_revenue        -   revenue per warehouse
-- inventory_turnover   -   total _quantity / avg_inventory
-- ============================================================
create or replace dynamic table gold_sch.agg_inventory_turnover;
    lag = downstream
    warehouse = fb_pipeline_wh
    comment = 'Inventory turnover KPIs by warehouse and category. DOWNSTREAM from AGG_BASE'
as 
select
    warehouse_id,
    warehouse_location,
    category,
    ---order volume
    count(order_id) as total_orders,
    sum(quantity) as total_quantity_ordered,
    ---Inventory metrics
    round(avg(inventory_level), 2) as avg_inventory_level,
    min(inventory_level) as min_inventory_level,
    max(inventory_level) as max_inventory_level,
    ---Revenue
    round(sum(total_amount), 2) as total_revenue,
    ---Inventory turnover ratio
    ---High = faster moving inventory
    round(sum(quantity)/nullif(avg(inventory_level),0),2) as inventory_turnover_ratio
from gold_sch.agg_base
group by 
     warehouse_id,
     warehouse_location,
     category;
select * from gold_sch.agg_inventory_turnover;
-- ============================================================
-- Step 5 - AGG_SHIPMNET_DELAYS (Downstream)
-- Purpose  : shipment delay KPIs by carrier
-- Source   : AGG_BASE (not silver directly)
-- Lag      : DOWNSTREAM - refreshes when AGG_BASE refreshes
-- Filter   : only shipped orders (shipment_id is not null
--            and shipment_id =! 'UNKNOWN')
-- KPIs     :
--    total_shipments      - shipment per carrier
--    on_time_shipmnets     - delay_days = 0
--     delayed_shipments    - delay_daays > 0
--      on_time_rate        - on time / total * 100
--      avg_delay_days      - average delay across all shipments
--      max_delay_days      - worst delay
--      total_revenue       - revenue per carrier
-- ============================================================
create or replace dynamic table gold_sch.agg_shipment_delays
    lag = downstream
    warehouse = fb_pipeline_wh
    comment = 'Shipment delay KPIs by carrier, DOWNSTREAM from AGG_BASE, only shipped orders included'
as
select
    carrier,
    customer_region,
    --Shipment counts--
    count(order_id) as total_shipments,
    count(case when delay_days = 0 then 1 end) as on_time_shipments,
    count(case when delay_days >0 then 1 end) as delayed_shipments,
    --On time rate--
    round(
        count(case when delay_days = 0 then 1 end) / nullif(count(order_id), 0) * 100,2) as on_time_rate_pct,
    --Delay metrics
    round(avg(delay_days), 2) as avg_delay_days,
    max(delay_days)  as max_delay_days,
    -- Revenue
    round(sum(total_amount), 2) as total_revenue
from gold_sch.agg_base
-- Only include shipped orders
-- Null shipment_id = order not yet shipped
where shipment_id is not null
group by 
     carrier,
    customer_region;
select * from gold_sch.agg_shipment_delays;
-- ============================================================
-- Step 6 - VERIFY

-- ============================================================
-- Check all dynamic tables created
show dynamic tables in schema gold_sch;

-- Check dynamic table refresh history
select * from table(information_schema.dynamic_table_refresh_history())
where schema_name =  'GOLD_SCH' and database_name = 'FB_DEV_DB'
order by refresh_start_time desc
limit 10;