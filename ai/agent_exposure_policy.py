# Databricks script -- Investment Exposure & Policy Assistant.
#
# Answers questions such as:
#   "Does GULF_EQ breach the banking concentration limit, by how much, and which
#    policy clause explains it?"
# by combining three things:
#   1. governed breach data  (Unity Catalog function tool / Gold table)
#   2. relevant policy text  (retrieved from Databricks AI Search -- RAG)
#   3. a Databricks foundation model that writes the final grounded answer.
#
# In production the governed functions are exposed to the agent through a Databricks
# MANAGED MCP SERVER (the recommended pattern for new agent tools). To keep this demo
# simple and readable, we call the same function directly via SQL and hand the
# results to the model as context.
#
# SECURITY BOUNDARY: structured breach/exposure data is read ONLY through the governed
# UC function (which enforces Unity Catalog row/column security). AI Search is used
# ONLY for APPROVED, non-restricted documents (IPS, risk guidelines, general
# research) -- AI Search does NOT support row/column permissions and cannot index a
# table with row filters/masks, so it must never hold portfolio-restricted content.

from pyspark.sql import SparkSession
from databricks.vector_search.client import VectorSearchClient
from mlflow.deployments import get_deploy_client

spark = SparkSession.builder.getOrCreate()
vsc = VectorSearchClient()
llm = get_deploy_client("databricks")

ENDPOINT = "investsphere_ai_search"
INDEX_NAME = "investsphere.ai.policy_index"
CHAT_MODEL = "databricks-meta-llama-3-3-70b-instruct"   # a Foundation Model endpoint


def get_breaches(portfolio_id):
    """Tool call: read this portfolio's breaches via the governed UC function."""
    rows = spark.sql(
        "SELECT * FROM investsphere.ai.get_portfolio_breaches('" + portfolio_id + "')"
    ).collect()
    return [row.asDict() for row in rows]


def search_policy(question, k=3):
    """RAG: retrieve the most relevant policy passages from AI Search."""
    index = vsc.get_index(endpoint_name=ENDPOINT, index_name=INDEX_NAME)
    results = index.similarity_search(
        query_text=question,
        columns=["doc_name", "chunk_text"],
        num_results=k,
    )
    return results["result"]["data_array"]   # list of [doc_name, chunk_text]


def build_prompt(question, breaches, passages):
    context = "BREACH DATA:\n" + str(breaches) + "\n\nPOLICY PASSAGES:\n"
    # AI Search returns each row as [doc_name, chunk_text, score] -- the similarity
    # score is appended as the last column, so take the first two by position rather
    # than unpacking to exactly two values.
    for row in passages:
        doc_name, chunk = row[0], row[1]
        context += "[" + str(doc_name) + "] " + str(chunk) + "\n"
    return (
        "You are an investment compliance assistant. Using ONLY the breach data and "
        "policy passages below, answer the question. Name the policy document you "
        "relied on. If the data does not support an answer, say so.\n\n"
        + context + "\nQUESTION: " + question
    )


def answer_question(question, portfolio_id):
    """The full assistant: tool call + RAG + grounded model answer."""
    breaches = get_breaches(portfolio_id)
    passages = search_policy(question)
    prompt = build_prompt(question, breaches, passages)
    response = llm.predict(
        endpoint=CHAT_MODEL,
        inputs={"messages": [{"role": "user", "content": prompt}], "max_tokens": 400},
    )
    return response["choices"][0]["message"]["content"]


if __name__ == "__main__":
    question = ("Does GULF_EQ breach the banking concentration limit, by how much, "
                "and which policy clause explains it?")
    print(answer_question(question, "GULF_EQ"))
