"""Write quarantined rows to a CSV so you can inspect bad data locally."""
import csv
import os


def write_quarantine_csv(rows, path):
    """Save quarantined rows (including the _dq_reasons) to a CSV file.

    Returns the number of rows written.
    """
    if not rows:
        return 0

    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    # collect every column name that appears in any row
    columns = set()
    for row in rows:
        for key in row:
            columns.add(key)
    columns = sorted(columns)

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            out = dict(row)
            # join the reasons list into one readable string
            out["_dq_reasons"] = "; ".join(row.get("_dq_reasons", []))
            writer.writerow(out)

    return len(rows)
