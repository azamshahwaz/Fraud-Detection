"""Run an external pipeline command while recording its status in PostgreSQL."""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from backend.src.common.db import pipeline_run
from backend.src.common.logging_setup import get_logger

log = get_logger("tracked_command")


def read_row_counts(result_file: Path) -> tuple[int, int]:
    """Read dbt result count and adapter row counts without guessing from log text."""
    if not result_file.is_file():
        return 0, 0
    try:
        results = json.loads(result_file.read_text(encoding="utf-8")).get("results", [])
    except (OSError, ValueError, TypeError):
        return 0, 0
    rows_out = 0
    for result in results:
        response = result.get("adapter_response") or {}
        affected = response.get("rows_affected", 0)
        if isinstance(affected, int) and not isinstance(affected, bool) and affected > 0:
            rows_out += affected
        elif isinstance(affected, str) and affected.isdigit():
            rows_out += int(affected)
    # dbt exposes output rows for adapters/models that report them, but not reliable input-row
    # totals. Keep rows_in at zero instead of substituting a count of result nodes.
    return 0, rows_out


def run_tracked(pipeline: str, command: list[str], cwd: Path,
                result_file: Path | None = None) -> int:
    if not pipeline or not command:
        raise ValueError("pipeline name and command are required")
    with pipeline_run(pipeline) as stats:
        completed = subprocess.run(command, cwd=cwd, check=False)
        if completed.returncode:
            raise RuntimeError(f"pipeline command failed (exit code {completed.returncode})")
        if result_file is not None:
            stats["rows_in"], stats["rows_out"] = read_row_counts(result_file)
    log.info("tracked pipeline command completed pipeline=%s", pipeline)
    return completed.returncode


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--result-file", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command and args.command[0] == "--" else args.command
    result_file = args.result_file
    if result_file is not None and not result_file.is_absolute():
        result_file = Path.cwd() / result_file
    return run_tracked(args.pipeline, command, Path.cwd(), result_file)


if __name__ == "__main__":
    raise SystemExit(main())
