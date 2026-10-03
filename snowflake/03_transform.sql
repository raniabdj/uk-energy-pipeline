-- UK Energy Pipeline — Snowflake version: silver + gold transforms
--
-- Uses LATERAL FLATTEN to explode the nested JSON arrays (regions, and each
-- region's fuel mix) directly in SQL — no separate parsing code needed, since
-- the raw JSON is already a native VARIANT column from the load step. This
-- is the same silver-layer logic as build_silver_regional() in the pandas
-- version and build_silver_regional() in the PySpark version, expressed as
-- Snowflake's native approach to semi-structured data instead.

USE WAREHOUSE uk_energy_wh;
USE DATABASE uk_energy_pipeline;

-- Silver: one row per region per fuel type, same shape the pandas and
-- PySpark versions produce.
CREATE OR REPLACE TABLE silver.regional_fuel_mix AS
SELECT
    region.value:regionid::INT              AS region_id,
    region.value:shortname::STRING          AS region_name,
    region.value:dnoregion::STRING          AS dno_region,
    region.value:intensity:forecast::INT    AS intensity_forecast,
    region.value:intensity:index::STRING    AS intensity_index,
    fuel.value:fuel::STRING                 AS fuel,
    fuel.value:perc::FLOAT                  AS pct_of_mix
FROM bronze.raw_regional,
    LATERAL FLATTEN(input => raw_payload:data[0]:regions) AS region,
    LATERAL FLATTEN(input => region.value:generationmix) AS fuel;

-- Gold: rank GB regions by carbon intensity — cleanest grid first. Same
-- aggregation as build_gold_regional_cleanest() in the other two versions.
CREATE OR REPLACE TABLE gold.regional_cleanliness_ranking AS
SELECT DISTINCT
    region_name,
    dno_region,
    intensity_forecast,
    intensity_index,
    RANK() OVER (ORDER BY intensity_forecast ASC) AS cleanliness_rank
FROM silver.regional_fuel_mix
ORDER BY cleanliness_rank;

-- Quick check
SELECT * FROM gold.regional_cleanliness_ranking LIMIT 10;
