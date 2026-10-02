# UK Carbon Intensity & Energy Mix Pipeline

A small end-to-end data engineering pipeline that pulls live grid data from the UK's public
[Carbon Intensity API](https://carbonintensity.org.uk/) and processes it through a
bronze → silver → gold medallion architecture, landing analysis-ready tables for Power BI.

No API key, no cloud account, and no cost required to run it — the API is free and public.

## Why this project

Most portfolio ETL projects stop at "pull some data and dump it in a CSV." This one
demonstrates the layered data modelling pattern (medallion architecture) used in
production data platforms like Databricks and Microsoft Fabric — the same architecture
I'm studying for the DP-700 (Fabric Data Engineer Associate) certification.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────┐
│  Carbon     │────▶│   Bronze     │────▶│   Silver     │────▶│   Gold    │
│  Intensity  │     │  (raw JSON,  │     │  (cleaned,   │     │ (business-│
│  API        │     │  as received)│     │   typed,     │     │  facing,  │
│             │     │              │     │   tidy)      │     │  Power BI)│
└─────────────┘     └──────────────┘     └──────────────┘     └───────────┘
   extract.py           data/bronze/        transform.py          data/gold/
                                             → data/silver/
                                             (parquet)             (csv)
```

- **Bronze** — raw API responses, landed exactly as received, timestamped. This is the
  source of truth; nothing is ever deleted or overwritten here.
- **Silver** — parsed into tidy, typed pandas DataFrames. One row per fact. Still
  granular, not yet aggregated.
- **Gold** — aggregated into the tables a dashboard actually connects to: national fuel
  mix summary, and a regional cleanliness ranking across GB's 17 grid regions.

## Stack

- **Extract**: Python + `requests`, hitting three public endpoints (national intensity,
  national generation mix, regional breakdown)
- **Transform**: `pandas` (this logic maps directly onto PySpark — swapping the engine
  for Databricks/Fabric is a drop-in change, the bronze/silver/gold contract doesn't change)
- **Storage**: local Parquet (silver) and CSV (gold) — in a cloud deployment this would be
  Azure Data Lake Storage Gen2, orchestrated by Azure Data Factory or a Fabric pipeline
- **Consumption**: Power BI connects directly to the `data/gold/` CSVs

## Running it

```bash
pip install -r requirements.txt
python src/extract.py      # pulls live data into data/bronze/
python src/transform.py    # builds data/silver/ and data/gold/
```

Run `extract.py` on a schedule (cron, GitHub Actions, or Azure Data Factory) to build up
a real historical dataset over time — each run lands a new timestamped file, so nothing
is overwritten.

## Roadmap

- [ ] Schedule `extract.py` via GitHub Actions for automated daily runs
- [ ] Swap local Parquet/CSV for Azure Data Lake Storage Gen2
- [ ] Orchestrate with Azure Data Factory / Microsoft Fabric pipelines
- [ ] Add a Power BI dashboard (screenshot + .pbix link here once built)
- [ ] Add dbt models for the silver → gold transformation instead of raw pandas
