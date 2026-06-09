# Databricks script  ---  incremental document refresh for RAG (on document upload)
#
# Triggered when a new/changed policy or research PDF is uploaded. Instead of
# REBUILDING the whole index, this does incremental AI data engineering:
#   parse + chunk the PDFs  ->  MERGE changed chunks into the Delta source table
#   ->  trigger a Databricks AI Search index sync (Triggered sync, cheaper than
#       Continuous for a personal project).
#
# Requires the one-time setup in ai/01_build_ai_search_index.py to have created the
# Delta source table + the Triggered-sync index.

import os

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from databricks.vector_search.client import VectorSearchClient

spark = SparkSession.builder.getOrCreate()

DOCS_VOLUME = "/Volumes/investsphere/ai/documents"
SOURCE_TABLE = "investsphere.ai.policy_chunks"
INDEX_NAME = "investsphere.ai.policy_index"
ENDPOINT = "investsphere_ai_search"

# Only index APPROVED, non-restricted documents (AI Search has no row/column security).
APPROVED_DOCS = {
    "investment_policy_statement.pdf",
    "portfolio_risk_guidelines.pdf",
    "listed_equity_research_note.pdf",
}


def load_chunks():
    from pypdf import PdfReader   # pip install pypdf
    rows = []
    for file_name in os.listdir(DOCS_VOLUME):
        if file_name not in APPROVED_DOCS:
            continue
        reader = PdfReader(os.path.join(DOCS_VOLUME, file_name))
        text = "\n".join((page.extract_text() or "") for page in reader.pages)
        for i, line in enumerate(ln.strip() for ln in text.split("\n") if ln.strip()):
            rows.append((file_name + "#" + str(i), file_name, line))
    return rows


new_chunks = spark.createDataFrame(load_chunks(), ["chunk_id", "doc_name", "chunk_text"])

# MERGE: insert new chunks, update changed ones (incremental, not a full rebuild)
(DeltaTable.forName(spark, SOURCE_TABLE).alias("t")
    .merge(new_chunks.alias("s"), "t.chunk_id = s.chunk_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# Trigger the AI Search Delta-sync index to pick up the changed chunks
index = VectorSearchClient().get_index(endpoint_name=ENDPOINT, index_name=INDEX_NAME)
index.sync()
print("documents merged into Delta + AI Search index sync triggered")
