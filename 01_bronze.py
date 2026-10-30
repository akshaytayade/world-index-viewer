# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: load raw quote snapshots into Delta
# MAGIC Reads `data/raw/<date>/quotes_*.json` from a Git folder (cloned from your GitHub repo)
# MAGIC and appends **only unseen batches** to `bronze_raw_quotes`, so reruns are idempotent.

# COMMAND ----------
dbutils.widgets.text("repo_path", "/Workspace/Users/<your-email>/market-map", "Git folder path")
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "market_map", "Schema")

REPO = dbutils.widgets.get("repo_path")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TABLE = f"{CATALOG}.{SCHEMA}.bronze_raw_quotes"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------
from pyspark.sql import types as T, functions as F
import glob, json, os

schema = T.StructType([
    T.StructField("iso3", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("continent", T.StringType()),
    T.StructField("index_name", T.StringType()),
    T.StructField("ticker", T.StringType()),
    T.StructField("market_status", T.StringType()),
    T.StructField("fetched_at", T.StringType()),
    T.StructField("last", T.DoubleType()),
    T.StructField("prev_close", T.DoubleType()),
    T.StructField("as_of", T.StringType()),
    T.StructField("pct_change", T.DoubleType()),
    T.StructField("quality", T.StringType()),
    T.StructField("error", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("source", T.StringType()),
])

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  iso3 STRING, country STRING, continent STRING, index_name STRING, ticker STRING,
  market_status STRING, fetched_at STRING, last DOUBLE, prev_close DOUBLE, as_of STRING,
  pct_change DOUBLE, quality STRING, error STRING, batch_id STRING, source STRING
) USING DELTA
""")

# COMMAND ----------
# Which batches are already loaded?
loaded = {r.batch_id for r in spark.table(TABLE).select("batch_id").distinct().collect()}

files = sorted(glob.glob(f"{REPO}/data/raw/*/quotes_*.json"))
new_files = [f for f in files if os.path.relpath(f, REPO) not in loaded]
print(f"{len(files)} snapshot files in repo, {len(new_files)} new")

# COMMAND ----------
rows = []
for f in new_files:
    batch_id = os.path.relpath(f, REPO)          # e.g. data/raw/2026-10-30/quotes_223012.json
    with open(f) as fh:
        for rec in json.load(fh):
            rows.append((
                rec.get("iso3"), rec.get("country"), rec.get("continent"),
                rec.get("index_name"), rec.get("ticker"), rec.get("market_status"),
                rec.get("fetched_at"), rec.get("last"), rec.get("prev_close"),
                rec.get("as_of"), rec.get("pct_change"), rec.get("quality"),
                rec.get("error"), batch_id, "yahoo",
            ))

if rows:
    spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(TABLE)
    print(f"Appended {len(rows)} rows from {len(new_files)} batches")
else:
    print("Nothing new to load")

# COMMAND ----------
display(spark.sql(f"SELECT batch_id, count(*) AS n, sum(quality='ok') AS ok FROM {TABLE} GROUP BY batch_id ORDER BY batch_id DESC LIMIT 10"))
