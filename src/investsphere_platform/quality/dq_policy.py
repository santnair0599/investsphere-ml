"""DQ policy tiers -- FAIL / QUARANTINE / WARN.

Mirrors the Spark logic in pipelines/silver_conform.py so the policy is unit-tested
without needing Spark. Same rule set (dq_rules.TRANSACTION_RULES), same reasons.

  FAIL       : a MANDATORY key (transaction_id) is missing -> raise, the Silver job
               must stop. A transaction with no id is unauditable, so we never
               silently drop OR quarantine it -- we fail loudly.
  QUARANTINE : a row breaks a business rule (bad FK / quantity / type) -> routed out
               with a quarantine_reason, preserved for audit and replay.
  WARN       : an optional field is missing -> handled at the Spark layer
               (@dp.expect / counted), not here.
"""
from investsphere_platform.quality import dq_runner

# Missing one of these is unauditable -> FAIL the pipeline (never quarantine).
MANDATORY_KEYS = ("transaction_id",)


class MandatoryFieldError(ValueError):
    """Raised when a mandatory key is missing -> the Silver job must fail."""


def enforce_mandatory_keys(rows, mandatory_keys=MANDATORY_KEYS):
    """Raise MandatoryFieldError if ANY row is missing a mandatory key.

    Returns the rows unchanged when every row has all mandatory keys.
    """
    for key in mandatory_keys:
        missing = sum(1 for row in rows if not row.get(key))
        if missing:
            raise MandatoryFieldError(
                "%d row(s) missing mandatory key '%s'" % (missing, key))
    return rows


def run_with_policy(rows, rules, ref, mandatory_keys=MANDATORY_KEYS):
    """Apply the tiered policy: FAIL on missing mandatory key, else split.

    Returns (valid, quarantined). Each quarantined row gains a `quarantine_reason`
    string (the joined _dq_reasons) so the reason persists into the Delta
    quarantine table exactly as the Spark job writes it.
    """
    enforce_mandatory_keys(rows, mandatory_keys)
    valid, quarantined = dq_runner.run_rules(rows, rules, ref)
    for row in quarantined:
        row["quarantine_reason"] = "; ".join(row.get("_dq_reasons", []))
    return valid, quarantined
