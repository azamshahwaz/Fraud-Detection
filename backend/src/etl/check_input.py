"""Fail fast (non-zero exit) if the PaySim input file is missing/empty or lacks required columns."""
import sys

import pandas as pd

from backend.src.common.config import get_settings
from backend.src.common.schema import REQUIRED_RAW


def main() -> int:
    p = get_settings().paysim_csv
    if not p.exists() or p.stat().st_size == 0:
        print(f"ERROR: {p} missing or empty. Download PaySim or run: python -m backend.src.data.generate_paysim", file=sys.stderr)
        return 1
    missing = [c for c in REQUIRED_RAW if c not in pd.read_csv(p, nrows=5).columns]
    if missing:
        print(f"ERROR: {p} lacks columns {missing}", file=sys.stderr)
        return 1
    print(f"input ok: {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
