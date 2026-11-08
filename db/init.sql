-- Run once as a superuser (docker-compose does this automatically via POSTGRES_* env).
-- Manual: psql -U postgres -f db/init.sql   (then set password with ALTER ROLE ... PASSWORD from your .env)
-- Schema itself is created by: python scripts/migrate.py
SELECT 'Database initialised by container env; run python scripts/migrate.py' AS note;
