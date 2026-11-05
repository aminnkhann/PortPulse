INSERT INTO source_system (source_id, priority, freshness_minutes)
VALUES ('digitraffic', 1, 120)
ON CONFLICT (source_id) DO UPDATE
SET priority = EXCLUDED.priority,
    freshness_minutes = EXCLUDED.freshness_minutes;
