DELETE FROM source_system
WHERE source_id IN ('digitraffic', 'portnet', 'ais')
  AND NOT EXISTS (
      SELECT 1 FROM raw_event
      WHERE raw_event.source_id = source_system.source_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM port_call_state
      WHERE port_call_state.source_id = source_system.source_id
  );
