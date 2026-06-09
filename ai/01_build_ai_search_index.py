# Databricks script -- build the RAG knowledge base with Databricks AI Search
# (formerly Databricks Vector Search). Run on Databricks with serverless enabled.
#
# Steps:
#   1. Read the policy/research documents into a Delta table, split into chunks.
#   2. Create an AI Search endpoint.
#   3. Create a Delta-sync index that embeds the chunks with a Databricks-hosted
#      embedding model, so the assistant can retrieve relevant passages.
#
# NOTE: AI Search needs a Unity Catalog-enabled workspace and serverless compute.
# The Python SDK is still imported as `databricks.vector_search` even though the
# product is now called AI Search. Check which embedding model endpoints exist in
# your workspace (this uses databricks-gte-large-en).

import os

from pyspark.sql import SparkSession
from databricks.vector_search.client import VectorSearchClient

spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE = "investsphere.ai.policy_chunks"
INDEX_NAME = "investsphere.ai.policy_index"
ENDPOINT = "investsphere_ai_search"
DOCS_VOLUME = "/Volumes/investsphere/ai/documents"   # upload data/documents/*.pdf here

# Only index APPROVED, non-restricted documents. AI Search cannot enforce row/column
# security, so confidential / portfolio-restricted material (e.g. the private
# investment committee memo) is intentionally EXCLUDED from the index.
APPROVED_DOCS = {
    "investment_policy_statement.pdf",
    "portfolio_risk_guidelines.pdf",
    "listed_equity_research_note.pdf",
}


def load_chunks():
    """Read the APPROVED PDFs and split each into chunks (one per non-empty line)."""
    from pypdf import PdfReader   # pip install pypdf  (use a richer chunker in production)

    rows = []
    for file_name in os.listdir(DOCS_VOLUME):
        if file_name not in APPROVED_DOCS:
            continue
        reader = PdfReader(os.path.join(DOCS_VOLUME, file_name))
        text = "\n".join((page.extract_text() or "") for page in reader.pages)
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        for i, line in enumerate(lines):
            rows.append((file_name + "#" + str(i), file_name, line))
    return rows


# 1. write chunks to a Delta table (change data feed is needed for delta-sync)
df = spark.createDataFrame(load_chunks(), ["chunk_id", "doc_name", "chunk_text"])
(df.write.format("delta").mode("overwrite")
   .option("delta.enableChangeDataFeed", "true")
   .saveAsTable(SOURCE_TABLE))

# 2. create the AI Search endpoint (safe to re-run)
vsc = VectorSearchClient()
try:
    vsc.create_endpoint(name=ENDPOINT, endpoint_type="STANDARD")
except Exception as error:
    print("endpoint may already exist:", error)

# 3. create the delta-sync index with managed embeddings
vsc.create_delta_sync_index(
    endpoint_name=ENDPOINT,
    index_name=INDEX_NAME,
    source_table_name=SOURCE_TABLE,
    primary_key="chunk_id",
    embedding_source_column="chunk_text",
    embedding_model_endpoint_name="databricks-gte-large-en",
    pipeline_type="TRIGGERED",
)
print("AI Search index created:", INDEX_NAME)
