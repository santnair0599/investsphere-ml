
# Databricks script  ---  BRONZE layer (Auto Loader, incremental)
#
# Improvements over a plain spark.read:
#   - Auto Loader (cloudFiles) ingests ONLY new files each run (incremental).
#   - Explicit schemas (no inferSchema) -> faster + fails loudly on schema drift.
#   - Mixed formats: reference/valuation data = CSV, transaction feeds = JSON.
#   - Audit columns stamped on every row.
#
# Layout expected on the volume (one sub-folder per source):
#   /Volumes/investsphere/bronze/raw/investment_transactions/*.json
#   /Volumes/investsphere/bronze/raw/listed_market_prices/*.csv
#   /Volumes/investsphere/bronze/raw/investment_asset_master/*.csv   ... one per source

from pyspark.sql import SparkSession
from investsphere_platform.ingestion.audit_columns import add_audit_columns
from investsphere_platform.ingestion.source_metadata import make_batch_id
from schemas import SCHEMAS, FORMATS
from job_params import get_param

spark = SparkSession.builder.getOrCreate()

RAW_PATH = "/Volumes/investsphere/bronze/raw"
CHECKPOINT = "/Volumes/investsphere/bronze/_checkpoints"
SCHEMA_LOC =  "/Volumes/investsphere/bronze/_schemas"

BRONZE = get_param("catalog", "investsphere") + ".bronze"
RUN_TS = get_param("run_ts", "2026-05-29T08:00")

# Any value that does not fit the explicit contract lands here instead of failing
# the stream or mutating the schema. Non-null _rescued_data => schema drift to review.
RESCUE_COL = "_rescued_data"

def ingest(table_name):
    file_format = FORMATS[table_name]
    reader = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", file_format)
              .option("cloudFiles.schemaLocation", SCHEMA_LOC + "/" + table_name)
              .option("cloudFiles.schemaEvolutionMode", "rescue")
              .option("rescuedDataColumn", RESCUE_COL)
              .schema(SCHEMAS[table_name]))
    if file_format == "csv":
        reader = reader.option("header", "true")
    
    df = reader.load(RAW_PATH + "/" + table_name)
    df = add_audit_columns(df, table_name + "." + file_format,
                           make_batch_id(table_name, RUN_TS))
    
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", CHECKPOINT + "/" + table_name)
             .option("mergeSchema", "true")
             .trigger(availableNow=True)
             .toTable(BRONZE + ".raw_" + table_name))
    print("started stream for", table_name, "(", file_format, ")")
    # Return the running query so the caller can WAIT for it (see below).
    return query


# writeStream is ASYNCHRONOUS: each call above only STARTS a background query and
# returns immediately. trigger(availableNow=True) means the query processes the files
# already present and then stops on its own -- but we must still WAIT for it. Without
# awaitTermination, this script's process exits first, the streams are killed before
# they commit, and the job reports "success" with NO table written. So: start every
# stream, then block until each finishes (and surface any stream error).
queries = [ingest(name) for name in SCHEMAS]
for query in queries:
    query.awaitTermination()

print("bronze ingest complete:", len(queries), "tables written")
