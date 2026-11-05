ALTER TABLE port_call_state
    DROP CONSTRAINT IF EXISTS port_call_state_status_check;

ALTER TABLE port_call_state
    ADD CONSTRAINT port_call_state_status_check
    CHECK (status IN ('TRUSTED', 'UNAVAILABLE_STALE', 'UNAVAILABLE'));

ALTER TABLE port_call_state
    DROP CONSTRAINT IF EXISTS port_call_state_check;

ALTER TABLE port_call_state
    ADD CONSTRAINT port_call_state_check
    CHECK (
        (status = 'TRUSTED'
            AND trusted_eta_utc IS NOT NULL
            AND source_id IS NOT NULL
            AND winning_event_id IS NOT NULL)
        OR
        (status IN ('UNAVAILABLE_STALE', 'UNAVAILABLE')
            AND trusted_eta_utc IS NULL
            AND source_id IS NULL
            AND winning_event_id IS NULL)
    );
