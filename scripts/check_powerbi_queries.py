"""Run every query in powerbi/queries.sql against PostgreSQL (what Power BI will import). Exit 1 on failure."""
import re
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.src.common.db import get_engine  # noqa: E402

blocks = re.split(r"^-- name: (\w+)\s*$", (Path(__file__).resolve().parents[1] / "powerbi" / "queries.sql").read_text(), flags=re.M)
failed = 0
with get_engine().connect() as c:
    for name, sql in zip(blocks[1::2], blocks[2::2]):
        try:
            res = c.execute(text(sql.strip().rstrip(";")))
            rows = res.fetchall()
            print(f"PASS {name}: {len(rows):,} rows, {len(res.keys())} columns")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {name}: {exc}")
sys.exit(1 if failed else 0)
