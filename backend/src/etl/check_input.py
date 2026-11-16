"""Validate a PaySim source before passing it to the batch ETL."""
from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from backend.src.common.config import get_settings
from backend.src.common.schema import OPTIONAL_RAW, REQUIRED_RAW


class SourceValidationError(ValueError):
    """A safe-to-display source error that never includes transaction values."""


@dataclass(frozen=True)
class SourceIdentity:
    batch_id: str
    source_path: str
    size_bytes: int
    sha256: str
    row_count: int
    headers: tuple[str, ...]

    def as_manifest(self) -> dict:
        result = asdict(self)
        result["headers"] = list(self.headers)
        return result


def inspect_source(source: Path | str) -> SourceIdentity:
    """Validate CSV structure and return stable identity metadata using bounded memory."""
    candidate = Path(source).expanduser()
    try:
        resolved = candidate.resolve(strict=True)
        before = resolved.stat()
    except (OSError, RuntimeError):
        raise SourceValidationError(f"source file not found or inaccessible: '{candidate.name}'") from None

    label = resolved.name or str(resolved)
    if not resolved.is_file():
        raise SourceValidationError(f"source is not a regular file: '{label}'")
    if before.st_size == 0:
        raise SourceValidationError(f"source file is empty: '{label}'")

    digest = hashlib.sha256()
    try:
        with resolved.open("rb") as source_file:
            for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
                digest.update(chunk)

        with resolved.open("r", encoding="utf-8-sig", newline="") as source_file:
            reader = csv.reader(source_file, strict=True)
            try:
                headers = next(reader)
            except StopIteration:
                raise SourceValidationError(f"source file is empty: '{label}'") from None

            if not headers or any(not name.strip() for name in headers):
                raise SourceValidationError(f"source has an empty CSV header: '{label}'")
            if len(set(headers)) != len(headers):
                raise SourceValidationError(f"source has duplicate CSV headers: '{label}'")

            missing = [name for name in REQUIRED_RAW if name not in headers]
            if missing:
                raise SourceValidationError(
                    f"source '{label}' is missing required CSV headers: {', '.join(missing)}"
                )
            unknown = [name for name in headers if name not in REQUIRED_RAW and name not in OPTIONAL_RAW]
            if unknown:
                raise SourceValidationError(
                    f"source '{label}' has unsupported CSV headers: {', '.join(unknown)}"
                )

            rows = 0
            for row in reader:
                # Spark also ignores fully blank CSV lines; keep that behavior consistent.
                if not row or (len(row) == 1 and row[0] == ""):
                    continue
                if len(row) != len(headers):
                    raise SourceValidationError(
                        f"source '{label}' has malformed CSV at line {reader.line_num}: "
                        f"expected {len(headers)} fields, found {len(row)}"
                    )
                rows += 1
            if rows == 0:
                raise SourceValidationError(f"source '{label}' contains headers but no transaction records")
    except SourceValidationError:
        raise
    except (OSError, UnicodeError, csv.Error):
        # Parser exceptions can include field contents. Never surface their messages.
        raise SourceValidationError(f"source '{label}' is unreadable or contains malformed CSV") from None

    try:
        after = resolved.stat()
    except OSError:
        raise SourceValidationError(f"source '{label}' changed or became inaccessible during validation") from None
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise SourceValidationError(f"source '{label}' changed during validation; retry with a stable file")

    checksum = digest.hexdigest()
    identity_path = os.path.normcase(str(resolved))
    batch_seed = json.dumps([identity_path, before.st_size, checksum], separators=(",", ":"))
    batch_id = hashlib.sha256(batch_seed.encode("utf-8")).hexdigest()
    return SourceIdentity(batch_id, str(resolved), before.st_size, checksum, rows, tuple(headers))


def main() -> int:
    source = get_settings().paysim_csv
    try:
        identity = inspect_source(source)
    except SourceValidationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"input ok: batch_id={identity.batch_id} size_bytes={identity.size_bytes} sha256={identity.sha256}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
