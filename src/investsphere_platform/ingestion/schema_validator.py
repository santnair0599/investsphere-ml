"""Check that an incoming dataset has the columns we expect."""


def missing_columns(actual_columns, expected_columns):
    """Return the list of expected columns that are NOT present."""
    missing = []
    for column in expected_columns:
        if column not in actual_columns:
            missing.append(column)
    return missing


def validate_columns(actual_columns, expected_columns):
    """Raise an error if any expected column is missing. Otherwise return True."""
    missing = missing_columns(actual_columns, expected_columns)
    if missing:
        raise ValueError("missing required columns: " + str(missing))
    return True
