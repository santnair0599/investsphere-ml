"""Data-quality rules for investment transactions.

Each rule is a simple function that takes one row (a dict) plus `ref` (reference
data to check against) and returns None if the row passes, or a short text reason
if it fails. No classes -- just functions. Add a rule and append it to
TRANSACTION_RULES at the bottom.
"""

# Trading/booking transaction types (cashflows and corporate actions live in their
# own feeds with their own types).
VALID_TRANSACTION_TYPES = {"BUY", "SELL", "ACQUISITION", "DISPOSAL"}


def rule_transaction_id_present(row, ref):
    if not row.get("transaction_id"):
        return "missing transaction_id"
    return None


def rule_known_portfolio(row, ref):
    if row.get("portfolio_id") not in ref["portfolios"]:
        return "unknown portfolio_id=" + str(row.get("portfolio_id"))
    return None


def rule_known_asset(row, ref):
    if row.get("investment_asset_id") not in ref["assets"]:
        return "unknown investment_asset_id=" + str(row.get("investment_asset_id"))
    return None


def rule_positive_quantity(row, ref):
    try:
        quantity = float(row.get("quantity"))
    except (TypeError, ValueError):
        return "non-numeric quantity"
    if quantity <= 0:
        return "non-positive quantity=" + str(quantity)
    return None


def rule_valid_transaction_type(row, ref):
    if row.get("transaction_type") not in VALID_TRANSACTION_TYPES:
        return "invalid transaction_type=" + str(row.get("transaction_type"))
    return None


# The runner applies these rules in order. Add new rules here.
TRANSACTION_RULES = [
    rule_transaction_id_present,
    rule_known_portfolio,
    rule_known_asset,
    rule_positive_quantity,
    rule_valid_transaction_type,
]
