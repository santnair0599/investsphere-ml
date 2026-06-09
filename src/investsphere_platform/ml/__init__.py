"""ML-platform helpers: pure-Python reference logic for model monitoring.

Mirrors the project's design idea (see README): the *logic* lives here as plain,
unit-testable functions with no Spark/Databricks dependency, while the Databricks
scripts (``ml/batch_inference.py``, ``ml/model_monitoring.py``) call this logic on
Spark-collected aggregates. That keeps the maths fast to test and the cluster code thin.
"""
