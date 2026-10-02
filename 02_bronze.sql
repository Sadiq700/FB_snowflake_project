-- Snowflake Supply Chain Project
-- Script : 02_bronze/bronze.sql
-- Purpose : bronze layer - storage integration, file formate,
--           external stage, RAW_ORDERS table, snowpipe
-- Run as : ACCOUNTADMIN -> then SYSADMIN

--Environment
use database fb_dev_db;

-- Storage Integration -- Rus as ACCOUNTADMIN
use role accountadmin;

create or replace storage integration fb_adls_integration
    type = external_stage
    storage_provider = 'azure'
    enabled = true
    azure_tenant_id = '5f1ac4d8-9506-4afa-bde6-312c800a6d32'
    storage_allowed_locations = (
    'azure://fbprojectadls.blob.core.windows.net/supply-chain-raw-dev/',
        'azure://fbprojectadls.blob.core.windows.net/supply-chain-raw-prod/'
    );
desc integration fb_adls_integration;

-- STEP 1 Run this & open AZURE_CONSENT_URL in browser & Assign Storage BLob Data Reader Role
--Azure Portal
--Storage Account
--Access Control (IAM)
--+ Add Add role assignment
--Role: Storage Blob Data Reader
--Members + Select members
--Search: (your AZURE_MULTI_TENANT_APP_NAME value)
--Select Review + assign

-- Grant to sysadmin
grant usage on integration fb_adls_integration to role sysadmin;

--IFICATION INTEGRATION (Event Grid → Storage Queue)
 
 /*Step 1 - Create Storage Queue
Azure Portal
Storage Account ()
Left menu scroll down
Data storage
Queue
Name: supply-chain-queue

Step 2 - Create Event Grid Subscription
Azure Portal
Storage Account ()
Left menu
Events
→ + Event Subscription
--Fill in:
Name : supply-chain-snowpipe-sub
Event Schema : Event Grid Schema
Event Types: Blob Created ONLY (uncheck everything else)

Endpoint Type : Storage Queues
Endpoint      : → Select an endpoint
                  Select storage account
                  Select queue: supply-chain-queue
                  Confirm selection
                  Click Create

Step 3 - Verify Event Grid Is Working
Azure Portal
→ Storage Account
Events
You should see 2 event subscriptions listed for dev & prod with filer enabled which starts with /blobServices/default/containers/supply-chain-raw/ and /blobServices/default/containers/supply-chain-raw-prod/

Step 4 - Come Back to Snowflake
Once queue and Event Grid are set up, run the Notification Integration with the correct queue URL: sqLAZURE_STORAGE_QUEUE_PRIMARY_URI=
''
*/
create or replace notification integration fb_azure_notifications_int
    enabled = true
    type = queue
    notification_provider = azure_storage_queue
    azure_storage_queue_primary_uri = 'https://fbprojectadls.queue.core.windows.net/fb-supply-chain-queue'
    azure_tenant_id = '5f1ac4d8-9506-4afa-bde6-312c800a6d32';
    
--Run this → copy AZURE_CONSENT_URL → open in browser Accept
--Copy AZURE_MULTI_TENANT_APP_NAME → assign Storage Queue Data Contributor in Azure IAM
desc integration fb_azure_notifications_int;

--Grant to SYSADMIN
grant usage on integration fb_azure_notifications_int to role sysadmin;

--switch to SYSADMIN
use role sysadmin;
use database fb_dev_db;
use schema fb_dev_db.bronze_sch;
use warehouse fb_pipeline_wh;

--File Format
create file format if not exists  bronze_sch.json_file_format
    type = 'json'
    strip_outer_array = true -- [ ]
    comment = 'JSON file format for flowbridge project';
    
desc file format bronze_sch.json_file_format;

--External Stage

create stage if not exists bronze_sch.adls_raw_stage
    url = 'azure://fbprojectadls.blob.core.windows.net/supply-chain-raw-dev/'
    storage_integration = fb_adls_integration
    file_format = bronze_sch.json_file_format
    comment = 'External stage - ADLS Gen2 DEV container';

list @bronze_sch.adls_raw_stage

--RAW_ORDERS TABLE
--Transient - no fail-safe, cost efficient, reloadable
--Metadata columns added at ingestion time by Snowpipe
create or replace transient table bronze_sch.raw_orders(
            raw_data variant,
    ingested_at timestamp_ntz default current_timestamp(),
    file_name string,
    file_row_number number,
    load_id string default uuid_string()
) comment = 'bronze layer raw JSON supply chain orders for flowbridge';

--SNOWPIPE
--Auto-ingest triggered by Azure Event Grid on file arrival
create pipe if not exists bronze_sch.supply_chain_pipe
    auto_ingest = true
    integration = fb_azure_notifications_int
    comment = 'Snowpipe - auto ingest json file from ADLS Gen2'
as
    copy into bronze_sch.raw_orders (
        raw_data,
        file_name,
        file_row_number
    )
from (
    select $1,
            metadata$filename,
            metadata$file_row_number
    from @bronze_sch.adls_raw_stage
) file_format = (format_name = 'bronze_sch.json_file_format');

--desc table bronze_sch.raw_orders

--desc file format bronze_sch.json_file_format

--Step 8 - VERIFY

-- Check pipe created
show pipes;

-- Check Snowpipe status
select system$pipe_status ('bronze_sch.supply_chain_pipe');

--After File land check data loaded
-- Since your 3 files landed before Snowpipe was created, Event Grid missed them. You need to manually refresh:
alter pipe bronze_sch.supply_chain_pipe refresh;

select * from bronze_sch.raw_orders;

--Check ingestion history
select * 
    from table (information_schema.copy_history(
                        table_name => 'raw_orders',
                        start_time => dateadd(hours, -1,current_timestamp())
                    ));