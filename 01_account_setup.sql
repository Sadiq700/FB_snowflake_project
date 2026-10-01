-- Snowflake Supply Chain Project
-- Script : 01_setup/account_setup.sql
-- Purpose : AccountLevel setup - databases, schemas,
--           warehouses, resource monitors
-- Run as : ACCOUNTADMIN

use role accountadmin;
--DATABASE
    create or replace database fb_dev_db
        comment = 'FB supply chain - Development Database';

    create or replace database fb_prod_db
        comment = 'FB supply chain - Production Database';

--Schemas - DEV

create or replace schema fb_dev_db.bronze_sch
    comment = 'Raw Ingetion Layer';

create or replace schema fb_dev_db.silver_sch
    comment = 'Transformation Layer';

create or replace schema fb_dev_db.gold_sch
    comment = 'Aggregation Layer';

create or replace schema fb_dev_db.serving_sch
    comment = 'Serving Layer';

--Schemas - PROD
create or replace schema fb_prod_db.bronze_sch
    comment = 'Raw Ingetion Layer';

create or replace schema fb_prod_db.silver_sch
    comment = 'Transformation Layer';

create or replace schema fb_prod_db.gold_sch
    comment = 'Aggregation Layer';

create or replace schema fb_prod_db.serving_sch
    comment = 'Serving Layer';

--Warehouses
-- Shared across DEV + PROD
-- Pipeline WH - Snowpipe + Streams + Tasks + Dynamic Tables
create warehouse if not exists fb_pipeline_wh
    warehouse_size = 'x-small'
    auto_suspend = 60
    auto_resume = true
    comment = 'Pipeline workloads - ingestion + transsformation';

-- Analytics WH - Streamlit + Secure views + Reader Account
create warehouse if not exists fb_analytics_wh
    warehouse_size = 'x-small'
    auto_suspend = 60
    auto_resume = true
    comment = 'Pipeline workloads - streamlit + data sharing';

--Resources Monitors
-- Pipeline warehouse monitor
create or replace resource monitor fb_pipeline_rm
    with credit_quota = 20
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent do notify
        on 90 percent do notify
        on 100 percent do suspend;

--Analaytics warehouse moniter
create or replace resource monitor fb_analytics_rm
    with credit_quota = 20
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent do notify
        on 90 percent do notify
        on 100 percent do suspend;
--Assign moniters to warehouses
alter warehouse fb_pipeline_wh to rol set resource_monitor = fb_pipeline_rm;
alter warehouse fb_analytics_wh set resource_monitor = fb_analytics_rm;

-- Grant privileges to sysadmin
use role accountadmin;
grant execute task on account to role sysadmin;

grant usage on warehouse fb_pipeline_wh to role sysadmin;
grant usage on warehouse fb_analytics_wh to role sysadmin;

grant all privileges on database fb_dev_db to role sysadmin;
grant all privileges on database fb_prod_db to role sysadmin;

grant all privileges on all schemas
    in database fb_dev_db to role sysadmin;
grant all privileges on all schemas
    in database fb_prod_db to role sysadmin;
--verify
show databases like 'fb_%';
show warehouses like 'fb_%';
show resource monitors;
