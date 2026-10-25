# FlowBridge-Snowflake-Project
Real-time supply chain analytics pipeline on Snowflake and Azure — Bronze to Gold medallion architecture, Snowpipe auto-ingestion, Streams, Tasks, Stored Procedures, Dynamic Tables, Secure Views, Streamlit dashboard, Pipeline Alerts, Data Sharing, Reader Account and zero-copy DEV to PROD promotion. Python + Faker simulates nested JSON source data

---

This project demonstrates how to build a real-time end-to-end supply chain analytics pipeline from scratch using Snowflake, Azure and Python.

Used a fictitious company — FlowBridge International Ltd. — a global industrial process equipment distributor.

──────────────🏗️ PROJECT ARCHITECTURE & COMPONENTS ──────────────

📦 DATA GENERATION → Python + Faker generates nested JSON supply chain orders → Azure SDK uploads files to Azure ADLS Gen2 → Historical mode: 300 records per batch → Continuous mode: 100 records every 30s

☁️ AZURE SETUP → Azure ADLS Gen2 — landing zone for JSON files → Azure Event Grid + Storage Queue — triggers Snowpipe on file arrival

❄️ SNOWFLAKE ACCOUNT SETUP → 2 Databases (DEV + PROD), 4 Schemas each → 2 Virtual Warehouses + 2 Resource Monitors → Storage Integration + Notification Integration

🥉 BRONZE LAYER → External Stage + JSON File Format → Transient table stores raw JSON as Variant with metadata → Snowpipe — auto-ingest on file arrival. No polling.

🥈 SILVER LAYER — STAGE 1 → Append-Only Stream + Task → Stored Procedure — flattens JSON, validates, cleans data → Permanent table for clean records + Transient table for rejected records

🥈 SILVER LAYER — STAGE 2 — STAR SCHEMA → Standard Stream + Task → Stored Procedure — Temporary table, 6 MERGEs, Sequences for surrogate keys → 5 Permanent Dimension tables — surrogate keys, SCD Type 1 (overwrite-based updates) → Permanent Fact table — 5 FKs, measures, dedup

🥇 GOLD LAYER — DYNAMIC TABLES → Base Dynamic Table — LAG = 1 MIN, joins all Silver tables → 4 Downstream Dynamic Tables — Order Fulfillment, Supplier Performance, Inventory Turnover, Shipment Delays → Silver queried once. Half the compute cost.

🔭 SERVING LAYER → 4 Secure Views on Gold Dynamic Tables → Analysts query views not tables. Required for Data Sharing.

📊 STREAMLIT DASHBOARD → Built natively in Snowflake. 60-second refresh. → Trends, Breakdown and Suppliers tabs. Live KPIs.

🔐 GOVERNANCE → Email Notification Integration → Pipeline Health Alert — Bronze + Silver + Gold every 5 mins → Data Share + Reader Account — partners query live data

🚀 DEV TO PROD → Zero-copy clone — full promotion in 60 seconds → PROD Snowpipe, Streams, Tasks and Alerts recreated

────────────── SNOWFLAKE OBJECTS COVERED ──────────────

Database · Schema · Warehouse · Resource Monitor · Storage Integration · Notification Integration · External Stage · File Format · Snowpipe · Transient Table · Permanent Table · Temporary Table · Variant · Stream · Task · Stored Procedure · Sequence · Dynamic Table · Secure View · Streamlit · Alert · Data Share · Reader Account · Zero-Copy Clone

#Snowflake #DataEngineering #AzureDataLake #Snowpipe #SnowflakeStreams #SnowflakeTasks #StoredProcedures #DynamicTables #SecureViews #DataSharing #Streamlit #Python #ETL #StarSchema #RealTimeData #SupplyChain #DataToCrunch #ZeroCopyClone #JSON
