# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.1"]
# ///
"""Build shop.duckdb: load the source system, then run the models in DAG order (staging, then marts)."""
from pathlib import Path

import duckdb

here = Path(__file__).parent
con = duckdb.connect(str(here / "shop.duckdb"))
con.execute("create schema if not exists staging; create schema if not exists mart")
for f in [here / "source_system/seed.sql", *sorted((here / "models/staging").glob("*.sql")), *sorted((here / "models/marts").glob("*.sql"))]:
    con.execute(f.read_text())
    print("ran", f.relative_to(here))
