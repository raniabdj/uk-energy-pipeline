# Databricks notebook source
# MAGIC %md
# MAGIC # UK Energy Pipeline — PySpark / Databricks version
# MAGIC
# MAGIC A PySpark reimplementation of this project's silver/gold transform logic, built to run
# MAGIC on [Databricks Community Edition](https://www.databricks.com/try-databricks) (free,
# MAGIC no billing required).
# MAGIC
# MAGIC The bronze layer (raw JSON from the UK Carbon Intensity API) is identical to the main
# MAGIC `src/extract.py` script in this repo — this notebook starts from that same raw data and
# MAGIC rebuilds the silver/gold layers using Spark DataFrames instead of pandas, demonstrating
# MAGIC the same medallion architecture at the engine Databricks/Fabric pipelines actually run on.
# MAGIC
# MAGIC **To run this yourself:**
# MAGIC 1. Sign up for Databricks Community Edition (free)
# MAGIC 2. Import this file as a notebook (Workspace > Import > upload this .py file)
# MAGIC 3. Upload the bronze JSON files from `data/bronze/` in the main repo to DBFS
# MAGIC    (Data > DBFS > Upload, into `/FileStore/bronze/`), or run `src/extract.py`
# MAGIC    locally first to generate them
# MAGIC 4. Attach to a cluster and run all cells

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# On Databricks, `spark` is already available in the notebook session.
# This line exists so the notebook also runs locally for testing.
spark = SparkSession.builder.appName("uk-energy-pipeline-pyspark").getOrCreate()

BRONZE_PATH = "/FileStore/bronze"  # Databricks DBFS path; adjust if running locally

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze → Silver: regional carbon intensity
# MAGIC
# MAGIC Reads the raw `regional_*.json` bronze file and flattens it into one row per
# MAGIC region per fuel type — the same shape `build_silver_regional()` produces in
# MAGIC `src/transform.py`, but expressed as Spark DataFrame operations instead of pandas.

# COMMAND ----------

def build_silver_regional(spark, bronze_json_path: str):
    """Read a regional bronze JSON file and flatten it into a tidy Spark DataFrame."""
    raw_df = spark.read.option("multiLine", True).json(bronze_json_path)

    # The API nests one row of regions under data[0].regions[] — explode it out
    regions = raw_df.select(F.explode(F.col("data")[0]["regions"]).alias("region"))

    exploded = regions.select(
        F.col("region.regionid").alias("region_id"),
        F.col("region.shortname").alias("region_name"),
        F.col("region.dnoregion").alias("dno_region"),
        F.col("region.intensity.forecast").alias("intensity_forecast"),
        F.col("region.intensity.index").alias("intensity_index"),
        F.explode(F.col("region.generationmix")).alias("fuel_mix"),
    )

    silver = exploded.select(
        "region_id",
        "region_name",
        "dno_region",
        "intensity_forecast",
        "intensity_index",
        F.col("fuel_mix.fuel").alias("fuel"),
        F.col("fuel_mix.perc").alias("pct_of_mix"),
    )
    return silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver → Gold: regional cleanliness ranking
# MAGIC
# MAGIC Same aggregation as `build_gold_regional_cleanest()` in the pandas version —
# MAGIC ranks every GB grid region by carbon intensity, cleanest first.

# COMMAND ----------

def build_gold_regional_cleanest(silver_regional_df):
    """Rank GB regions by carbon intensity — cleanest grid right now."""
    gold = (
        silver_regional_df
        .select("region_name", "dno_region", "intensity_forecast", "intensity_index")
        .distinct()
        .orderBy("intensity_forecast")
    )
    return gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run it
# MAGIC
# MAGIC Point this at a bronze regional JSON file (uploaded to DBFS, or a local path if
# MAGIC testing outside Databricks) and write the gold output as a Delta table —
# MAGIC the storage format Databricks lakehouse pipelines are built on.

# COMMAND ----------

if __name__ == "__main__":
    import sys
    bronze_file = sys.argv[1] if len(sys.argv) > 1 else f"{BRONZE_PATH}/regional_sample.json"

    silver_regional = build_silver_regional(spark, bronze_file)
    silver_regional.show(10, truncate=False)

    gold_regional = build_gold_regional_cleanest(silver_regional)
    gold_regional.show(20, truncate=False)

    # On Databricks, write as a managed Delta table:
    # gold_regional.write.format("delta").mode("overwrite").saveAsTable("gold.regional_cleanliness")
