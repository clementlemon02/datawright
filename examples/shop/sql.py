# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.1"]
# ///
"""Run one query against shop.duckdb, read-only, and print CSV: uv run sql.py "select ..." """
import csv
import sys
from pathlib import Path

import duckdb

try:
    cur = duckdb.connect(str(Path(__file__).parent / "shop.duckdb"), read_only=True).execute(sys.argv[1])
except duckdb.Error as e:
    sys.exit(str(e))
out = csv.writer(sys.stdout)
out.writerow([d[0] for d in cur.description])
out.writerows(cur.fetchall())
