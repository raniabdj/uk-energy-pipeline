# Snowflake version

A third implementation of this project's bronze/silver/gold pipeline, alongside the
pandas version (../src/) and the PySpark/Databricks version (../databricks/). Same
source data (the UK Carbon Intensity API's regional endpoint), same medallion
architecture — this version uses Snowflake's native semi-structured data handling
(VARIANT + LATERAL FLATTEN) to go from raw JSON to modeled tables without a separate
parsing step in application code.

Tested: this SQL was run end-to-end against a live Snowflake trial account —
real UK Carbon Intensity data loaded via COPY INTO, flattened with
LATERAL FLATTEN, and correctly ranked by region (North/South Scotland
cleanest, GB national average dirtiest).

## Why Snowflake here specifically

Snowflake's distinguishing feature versus a generic SQL warehouse is first-class
semi-structured data support — you can load raw JSON straight into a VARIANT column
with zero upfront schema, then query into its nested structure directly in SQL via
LATERAL FLATTEN.

## Running it

Requires a free Snowflake trial account (https://signup.snowflake.com/) — no credit
card required to start.

1. Run 01_setup.sql — creates a minimal (X-Small, auto-suspending) warehouse,
   database, schemas, and an internal stage
2. Run 02_load_data.sql — uploads this project's bronze JSON files to Snowflake
   and loads them into a raw VARIANT table
3. Run 03_transform.sql — flattens the nested JSON into silver and gold tables
   using LATERAL FLATTEN
