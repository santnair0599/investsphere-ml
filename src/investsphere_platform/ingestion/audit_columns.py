"""Add audit columns to a Spark DataFrame (used by the bronze pipeline).

PySpark is imported INSIDE the function, not at the top of the file. That way you
can import the rest of the library on your laptop without having Spark installed;
Spark is only needed when this function actually runs on Databricks.
"""


def add_audit_columns(df, source_file, batch_id):
    """Return the DataFrame with _ingest_ts, _source_file and _batch_id added."""
    from pyspark.sql import functions as F  # imported only when running on Spark

    return (
        df
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
    )
