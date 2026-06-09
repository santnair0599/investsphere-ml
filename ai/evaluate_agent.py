# Databricks script -- evaluate the assistant with MLflow GenAI evaluation.
#
# Runs a small set of questions through the assistant and scores the answers for
# correctness, relevance, and whether they cite a policy document, using
# LLM-as-judge scorers. This is the "evaluation" capability the role asks for.
#
# NOTE: MLflow's GenAI evaluation API is evolving (mlflow.genai in MLflow 3+).
# Verify the scorer names/imports in your workspace's MLflow version. The shape
# below reflects the current documented pattern.

import mlflow
from mlflow.genai.scorers import Correctness, RelevanceToQuery, Guidelines

from agent_exposure_policy import answer_question

# small evaluation set: each item has the question + the facts a good answer needs
eval_data = [
    {
        "inputs": {"question": "Does GULF_EQ breach the banking concentration limit?",
                   "portfolio_id": "GULF_EQ"},
        "expectations": {"expected_facts": ["GULF_EQ", "Banking", "20%", "breach"]},
    },
    {
        "inputs": {"question": "Is GLOBAL_BAL within its technology limit?",
                   "portfolio_id": "GLOBAL_BAL"},
        "expectations": {"expected_facts": ["Technology", "40%"]},
    },
]


def predict_fn(question, portfolio_id):
    return answer_question(question, portfolio_id)


results = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=predict_fn,
    scorers=[
        Correctness(),
        RelevanceToQuery(),
        Guidelines(name="cites_policy",
                   guidelines="The answer must name the policy document it relied on."),
    ],
)

print("Evaluation complete. Open the MLflow run in the UI for per-question scores.")
print(results.metrics)
