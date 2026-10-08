-- ================================================
-- Snowflake Supply Chain Project
-- Script   : 05_serving/serving.sql
-- Purpose  : Serving Layer - Secure Views over Gold
--          : Dynamic Tables for governed access
-- Run as   : SYSADMIN
-- Note     : Run each step individually, not all at once
-- Patter   : Secure views om gold dynamic tables
--          : Analysts query views - never tables directly 
--            gold dynamic tables handle refresh automatically
--            No extra serving tables needed
-- ================================================
 --Environment
  use role sysadmin;
  use database fb_dev_db;
  use schema serving_sch;
  use warehouse fb_analytics_wh;
--  ==============================================================
-- STEP 1 - VW_ORDER FULFILLMENT 
-- Purpose : Governed access to order fulfillment KPIs 
-- Source : GOLD SCH.AGG ORDER FULFILLMENT (Dynamic Table) 
-- Type : Secure View - view definition hidden from users 
--        who don't own the view
-- Used by : Streamlit dashboard + Reader Account
--  ==============================================================
create or replace secure view serving_sch.vw_orders_fulfillment
    comment = 'Secure view for GOLD_SCH.AGG_ORDER_FULFILMENT (Dynamic Table)'
as
select 
    customer_region,
    customer_segment,
    total_orders,
    delivered_orders,
    pending_orders,
    cancelled_orders,
    in_transit_orders,
    fulfillment_rate_pct,
    total_revenue,
    avg_order_value,
    paid_orders,
    payment_pending_orders,
    overdue_orders
from gold_sch.agg_order_fulfillment;

--  ==============================================================
-- STEP 2 - VW SUPPLIER PERFORMANCE 
-- Purpose : Governed access to supplier performance KPIs
-- Source : GOLD SCH.AGG_ SUPPLIER_PERFORMANCE (Dynamic Table)
-- Type : Secure View 
-- Used by : Streamlit dashboard + Reader Account
--  ==============================================================
create or replace secure view serving_sch.vw_supplier_performance
    comment = 'Secure View for GOLD_SCH>AGG_SUPPLIER_PERFORMANCE'
as
select
    supplier_id,
    supplier_name,
    supplier_country,
    total_orders,
    avg_performance_score,
    avg_lead_time_days,
    total_revenue,
    avg_order_value,
    on_time_orders,
    delayed_orders,
    on_time_rate_pct,
    avg_delay_days
from gold_sch.supplier_performance;

--  ==============================================================

-- STEP 3 - VW TNVENTORY TURNOVER
-- Purpose : Governed access to inventory KPIs 
-- Source : GOLD SCH.AGG INVENTORY_TURNOVER (Dynamic Table) 
-- Type : Secure View 
-- Used by : Streamlit dashboard + Reader Account
--  ==============================================================
create or replace secure view serving_sch.vw_inventory_level_turnover
    comment = 'Secure View for GOLD_SCH.AGG_INVENTORY_TURNOVER'
as
select
    warehouse_id,
    warehouse_location,
    category,
    total_orders,
    total_quantity_ordered,
    avg_inventory_level,
    min_inventory_level,
    max_inventory_level,
    total_revenue,
    inventory_turnover_ratio
from gold_sch.agg_inventory_turnover;

--  ==============================================================
-- STEP 4 - VW SHIPMENT DELAYS 
-- Purpose : Governed access to shipment delay KPIs
-- Source : GOLD SCH.AGG_SHIPMENT_DELAYS (Dynamic Table) 
-- Type : Secure View
-- Used by : STreamlit dashboard + Reader Account
--  ==============================================================
create or replace secure view serving_sch.vw_shipment_delays
    comment = ' Secure View for GOLD_SCH.AGG_SHIPMENT_DELAYS'
as
select
    carrier,
    customer_region,
    total_shipments,
    on_time_shipments,
    delayed_shipments,
    on_time_rate_pct,
    avg_delay_days,
    max_delay_days,
    total_revenue
from gold_sch.agg_shipment_delays;

--  ==============================================================
-- Step 5 - VERIFY
--  ==============================================================
-- Check all views created
show views in schema serving_sch;

-- Perview all views
select * from serving_sch.vw_shipment_delays;