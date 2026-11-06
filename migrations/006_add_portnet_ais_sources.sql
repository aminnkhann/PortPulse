-- These priorities are demo rules, not claims about real-world source authority.
INSERT INTO source_system (source_id, priority, freshness_minutes)
VALUES
    ('portnet', 2, 120),
    ('ais', 1, 120)
ON CONFLICT (source_id) DO UPDATE
SET priority = EXCLUDED.priority,
    freshness_minutes = EXCLUDED.freshness_minutes;
