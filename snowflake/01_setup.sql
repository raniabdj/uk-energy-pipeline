-- UK Energy Pipeline — Snowflake version: setup
--
-- A third implementation of this project's bronze/silver/gold pipeline,
-- alongside the pandas version (../src/) and the PySpark/Databricks version
-- (../databricks/). Same source data, same medallion architecture — this
-- version uses Snowflake's native semi-structured data handling, which is
-- one of its defining strengths, to go straight from raw JSON to queryable
-- tables without a separate parsing step.

-- Minimal compute: X-Small is Snowflake's smallest warehouse size, and
-- AUTO_SUSPEND stops it after 60 seconds idle so a free trial isn't billed
-- for compute sitting unused.
CREATE WAREHOUSE IF NOT EXISTS uk_energy_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS uk_energy_pipeline;
CREATE SCHEMA IF NOT EXISTS uk_energy_pipeline.bronze;
CREATE SCHEMA IF NOT EXISTS uk_energy_pipeline.silver;
CREATE SCHEMA IF NOT EXISTS uk_energy_pipeline.gold;

USE WAREHOUSE uk_energy_wh;
USE DATABASE uk_energy_pipeline;

-- An internal stage to upload the raw bronze JSON files to, before loading.
CREATE STAGE IF NOT EXISTS bronze.raw_json_stage
    FILE_FORMAT = (TYPE = JSON);

-- Bronze: one VARIANT column holds the entire raw JSON payload, exactly as
-- the Carbon Intensity API returned it — no parsing or schema imposed yet.
-- This mirrors the pandas/PySpark versions' bronze layer (raw JSON, as-is),
-- just stored as a native Snowflake semi-structured type instead of a file.
CREATE TABLE IF NOT EXISTS bronze.raw_regional (
    raw_payload   VARIANT,
    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
