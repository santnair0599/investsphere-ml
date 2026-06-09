"""InvestSphere reusable data-platform library.

Business rules (exposure, P&L, breach detection, SCD2, DQ) are kept as pure
Python so they unit-test without a Spark cluster. PySpark-specific helpers import
pyspark lazily, so importing this package never requires Spark to be installed.
"""

__version__ = "0.1.0"
