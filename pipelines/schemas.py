"""
This file defines schemas and source information for every dataset.

A schema means:
    "What columns does this file have, and what is the data type of each column?"

Example:
    portfolio_master has:
        portfolio_id     string
        portfolio_name   string
        manager          string
        mandate          string
        base_currency    string

Why we define schemas manually:
    - Faster than inferSchema
    - Safer for production
    - Prevents Spark from guessing wrong data types
    - Helps catch schema changes early
"""
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# -------------------------------------------------------
# 1. Helper function to create Spark schema
# -------------------------------------------------------

def create_schema(columns):
    """
    Create a Spark schema from a list of columns.

    Input example:
        [
            ("portfolio_id", StringType()),
            ("portfolio_name", StringType())
        ]

    Output:
        StructType([
            StructField("portfolio_id", StringType(), True),
            StructField("portfolio_name", StringType(), True)
        ])
    """
    schema_fields = []
    for column_name, column_type in columns:
        field = StructField(column_name, column_type, True)
        schema_fields.append(field)
    
    return StructType(schema_fields)

# -------------------------------------------------------
# 2. Short names for data types
# -------------------------------------------------------

STRING = StringType()
DOUBLE = DoubleType()

# -------------------------------------------------------
# 3. Schemas for all source files
# -------------------------------------------------------

SCHEMAS = {
    # ---------------------------------------------------
    # Reference data files
    # These are CSV files
    # ---------------------------------------------------

       "portfolio_master": create_schema([
        ("portfolio_id", STRING),
        ("portfolio_name", STRING),
        ("manager", STRING),
        ("mandate", STRING),
        ("base_currency", STRING)
    ]),
       
       "investment_asset_master": create_schema([
        ("investment_asset_id", STRING),
        ("name", STRING),
        ("asset_type", STRING),
        ("isin", STRING),
        ("ticker", STRING),
        ("internal_investment_code", STRING),
        ("issuer_id", STRING),
        ("sector", STRING),
        ("country", STRING),
        ("currency", STRING)
    ]),
       
       "issuer_master": create_schema([
        ("issuer_id", STRING),
        ("issuer_name", STRING),
        ("country", STRING),
        ("credit_rating", STRING)
    ]),

    "counterparty_master": create_schema([
        ("counterparty_id", STRING),
        ("name", STRING),
        ("type", STRING),
        ("risk_category", STRING),
        ("country", STRING)
    ]),
      "investment_limits": create_schema([
        ("limit_id", STRING),
        ("scope_type", STRING),
        ("scope_value", STRING),
        ("portfolio_id", STRING),
        ("max_pct", DOUBLE)
    ]),

    "benchmark_or_target_allocation": create_schema([
        ("benchmark_id", STRING),
        ("sector", STRING),
        ("target_weight_pct", DOUBLE)
    ]),
    
    "currency_rates": create_schema([
        ("rate_date", STRING),
        ("currency", STRING),
        ("rate_to_base", DOUBLE)
    ]),

    # ---------------------------------------------------
    # Transaction data files
    # These are JSON files
    # ---------------------------------------------------
    
     "investment_transactions": create_schema([
        ("transaction_id", STRING),
        ("transaction_date", STRING),
        ("portfolio_id", STRING),
        ("investment_asset_id", STRING),
        ("transaction_type", STRING),
        ("quantity", DOUBLE),
        ("price", DOUBLE),
        ("counterparty_id", STRING)
    ]),
     
     "cashflows": create_schema([
        ("cashflow_id", STRING),
        ("portfolio_id", STRING),
        ("cashflow_date", STRING),
        ("type", STRING),
        ("amount", DOUBLE),
        ("currency", STRING)
    ]),

    "corporate_actions": create_schema([
        ("action_id", STRING),
        ("investment_asset_id", STRING),
        ("action_type", STRING),
        ("ex_date", STRING),
        ("value", DOUBLE)
    ]),
    
    # ---------------------------------------------------
    # Valuation data files
    # These are CSV files
    # ---------------------------------------------------

    "listed_market_prices": create_schema([
        ("price_date", STRING),
        ("investment_asset_id", STRING),
        ("close_price", DOUBLE)
    ]),
    
      "investment_holdings_snapshot": create_schema([
        ("portfolio_id", STRING),
        ("as_of_date", STRING),
        ("investment_asset_id", STRING),
        ("quantity", DOUBLE)
    ]),

    "private_valuation_snapshot": create_schema([
        ("investment_asset_id", STRING),
        ("valuation_date", STRING),
        ("fair_value", DOUBLE),
        ("currency", STRING)
    ]),
    
     "fund_nav_snapshot": create_schema([
        ("investment_asset_id", STRING),
        ("nav_date", STRING),
        ("nav_per_unit", DOUBLE),
        ("currency", STRING)
    ])
}


# -------------------------------------------------------
# 4. File format for each source
# -------------------------------------------------------

FORMATS = {}
for table_name in SCHEMAS:
    if table_name in ["investment_transactions", "cashflows", "corporate_actions"]:
        FORMATS[table_name] = "json"
    else:
        FORMATS[table_name] = "csv"
        
# -------------------------------------------------------
# 5. Source folder category for each dataset
# -------------------------------------------------------

CATEGORY = {
    # Reference data
    "portfolio_master": "reference_data",
    "investment_asset_master": "reference_data",
    "issuer_master": "reference_data",
    "counterparty_master": "reference_data",
    "investment_limits": "reference_data",
    "benchmark_or_target_allocation": "reference_data",
    "currency_rates": "reference_data",

  # Transaction data
    "investment_transactions": "transaction_data",
    "cashflows": "transaction_data",
    "corporate_actions": "transaction_data",

    # Valuation data
    "listed_market_prices": "valuation_data",
    "investment_holdings_snapshot": "valuation_data",
    "private_valuation_snapshot": "valuation_data",
    "fund_nav_snapshot": "valuation_data"
}

