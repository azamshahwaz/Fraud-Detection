"""Apply db/migrations/*.sql in order, once each (tracked in schema_migrations)."""
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.src.common.db import get_engine  # noqa: E402
from backend.src.common.logging_setup import get_logger  # noqa: E402

log = get_logger("migrate")
MIGRATIONS = Path(__file__).resolve().parents[1] / "db" / "migrations"


def main() -> int:
    eng = get_engine()
    with eng.begin() as c:
        c.execute(text("CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now())"))
    applied = 0
    for f in sorted(MIGRATIONS.glob("*.sql")):
        with eng.begin() as c:
            if c.execute(text("SELECT 1 FROM schema_migrations WHERE name=:n"), {"n": f.name}).first():
                log.info("skip %s (already applied)", f.name)
                continue
            cur = c.connection.cursor()   # raw DBAPI cursor: no %-interpolation of the script
            cur.execute(f.read_text())
            cur.close()
            c.execute(text("INSERT INTO schema_migrations(name) VALUES (:n)"), {"n": f.name})
            log.info("applied %s", f.name)
            applied += 1
    log.info("migrations done (%d new)", applied)
    return 0


if __name__ == "__main__":
    sys.exit(main())
