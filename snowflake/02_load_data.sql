-- UK Energy Pipeline — Snowflake version: load bronze data
--
-- Run this from SnowSQL (Snowflake's CLI) or the Snowsight web UI, from the
-- directory containing the main project's data/bronze/*.json files.

-- PUT uploads a local file to the internal stage. SnowSQL only — in
-- Snowsight's web UI, use the "Upload files" option on the stage instead.
PUT file:///path/to/uk-energy-pipeline/data/bronze/regional_*.json
    @bronze.raw_json_stage
    AUTO_COMPRESS = TRUE;

-- COPY INTO loads the staged file(s) into the bronze table. Each JSON file
-- becomes one row with its full content in the VARIANT column — Snowflake
-- doesn't require a fixed schema to ingest JSON, unlike a traditional
-- relational COPY.
COPY INTO bronze.raw_regional (raw_payload)
FROM @bronze.raw_json_stage
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = 'CONTINUE';

-- Sanity check: confirm rows landed and peek at the raw structure.
SELECT COUNT(*) AS rows_loaded FROM bronze.raw_regional;
SELECT raw_payload FROM bronze.raw_regional LIMIT 1;
