-- Preserve low-sample drift checks as explicit, non-numeric statuses rather than misleading PSI values.
ALTER TABLE drift_reports ALTER COLUMN psi DROP NOT NULL;
ALTER TABLE drift_reports DROP CONSTRAINT IF EXISTS drift_reports_status_check;
ALTER TABLE drift_reports ADD CONSTRAINT drift_reports_status_check
    CHECK (status IN ('OK', 'WARN', 'DRIFT', 'INSUFFICIENT_DATA'));
