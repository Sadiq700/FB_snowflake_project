-- =================================================================================
-- Snowflake Supply Chain Project
-- Script   : 07_governance/governance.sql
-- Purpose  : Governance - Email Notification Integration, Pipeline Health Alerts
-- Rus as   : ACCOUNTADMIN
-- Note     : Run each step individually, not all at once

-- WHY GOVERNANCE
-- -------------------------
-- Without governance:
--  . Pipeline breaks at 2am -> nobody knows until morning
--  . Partner needs data -> manual file exports -> stale data
--  . No control over who sees what

-- With governance
--   . Pipeline alert fires immediately ->  fix before business starts
--   . Partner queries live data directly -> no csv exports 
--   . Full control over data access
-- =================================================================================
use role accountadmin;
use database fb_dev_db;
use warehouse fb_pipeline_wh;
-- =================================================================================
-- Step 1 - EMAIL NOTIFICATION INTEGRATION
------------------------------------------
-- What     : Connect Snowflake to an email server
-- Why      : Request before any alert can send emails
--              without this  -> alerts run but no email sent
-- Real Life : Every enterprise monitoring system needs
--              a notification channel - email is simplest
-- =================================================================================

create notification integration if not exists emial_notification_int
    type = email 
    enabled = true
    comment = 'Email Notification Integration for pipeline health alerts';

-- Test email - verify integration is working
select system$start_user_email_verification('sadiq');
call system$send_email(
    'emial_notification_int',
    'sadiqtech83@gmail.com',
    'Flowbridge Project - Supply Chain',
    'FB - Alerts are ready'
);

-- Verify integration created
show integrations;

--=================================================================================
-- STEP 2 - PIPELINE HEALTH ALERT
-- ---------------------------------
-- What      : Single alert that monitors ALL 3 pipeline layers 
--             Bronze (Snowpipe) + Silver (Tasks) + Gold (Dynamic Tables)
-- Why        : Real companies run pipelines 24/7 
--              Nobody manually checks logs every minute    
--              Alert detects failures automatically and notifies instantly
-- Schedule   :  Every 5 minutes - good balance of cost vs speed 
-- Condition  : If ANY failure found in Bronze/Silver/Gold 
--             -> send email immediately 
-- Real life   : On-call engineers get paged at 2am
--              They fix the issue before business starts 
--              Business never knows there was a problem 
--=================================================================================
create or replace alert bronze_sch.pipeline_health_alert
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
        ------------- Silver - Snowpipe Failure ----------
        select 1 from table (information_schema.task_history(
            scheduled_time_range_start = dateadd(hour, -1, current_timestamp())
        ))
        where state = 'FAILED'
        and database_name = 'fb_dev_db'

        union all
        ------------- Gold - Snowpipe Failure ----------
        select 1 from table(information_schema.dynamic_table_refresh_history())
        where schema_name = 'GOLD_SCH'
        and database = 'fb_dev_db'
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

    -- Activate alert - alerts are SUSPENDED by default
    alter alert bronze_sch.pipeline_health_alert resume;

    -- Verify alert is active 
    show alerts in database fb_dev_db;

    -- Check alert history
    select * from table (information_schema.alert_history(
        scheduled_time_range_start => dateadd(hour,-1,current_timestamp())
    ))
    order by scheduled_time desc
    limit 10;
    -- =================================================================================
    -- Step 3 - VERIFY governance objects
    ----------------------------------
    -- Note : Data Sharing & Reader Account are PROD-only
    --        see 08_dev_to_prod.sql (Step 8) for share setup
    --=================================================================================

    -- All alerts
    show alerts in database fb_dev_db;
    
    -- Resource monitors (created in account_setup.sql)
    show resource monitors;