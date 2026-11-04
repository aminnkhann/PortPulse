-- Preserve simulated source receipt time and record when the database stores the event.
ALTER TABLE raw_event
    ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ;

-- Existing rows cannot recover their original ingestion time; use migration time.
UPDATE raw_event
SET ingested_at_utc = CURRENT_TIMESTAMP
WHERE ingested_at_utc IS NULL;

ALTER TABLE raw_event
    ALTER COLUMN ingested_at_utc SET DEFAULT clock_timestamp(),
    ALTER COLUMN ingested_at_utc SET NOT NULL;
