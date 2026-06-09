"""Run the data-quality rules and split rows into good vs quarantined."""


def run_rules(rows, rules, ref):
    """Check every row against every rule.

    Returns two lists:
      valid        - rows that passed all rules
      quarantined  - rows that failed at least one rule. Each quarantined row gets
                     an extra "_dq_reasons" list saying what was wrong.
    """
    valid = []
    quarantined = []
    for row in rows:
        reasons = []
        for rule in rules:
            reason = rule(row, ref)
            if reason is not None:
                reasons.append(reason)
        if reasons:
            bad_row = dict(row)
            bad_row["_dq_reasons"] = reasons
            quarantined.append(bad_row)
        else:
            valid.append(row)
    return valid, quarantined
