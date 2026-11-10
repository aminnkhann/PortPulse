# Portnet-only live ingestion report — 2026-11-10

This report records one Portnet V1 API poll from the simplified Phase 2 branch. It is separate from `phase2_marine_live_ingestion_2026-11-09.md`, which documents the earlier multi-source Portnet/AIS experiment. No synthetic fault records were mixed into this live run; synthetic failures are covered separately in `phase2_portnet_injected_reliability_tests.md`.

Reproduce against a disposable PostgreSQL database with `DATABASE_URL` configured:

```sh
DATABASE_URL="${DATABASE_URL:?set to a disposable PostgreSQL database}" \
  python -m scripts.ingest_portnet --date 2026-11-10
```

## Run metrics

| Metric | Result |
| --- | ---: |
| Portnet V1 call records received | 105 |
| Active Portnet calls | 44 |
| Completed calls skipped | 61 |
| Calls without ETA skipped | 0 |
| Accepted ETA observations | 44 |
| Rejected call records | 0 |
| Rejected ETA details | 0 |
| Exact duplicate call records in response | 0 |
| Duplicate normalized event attempts in response | 0 |
| Unchanged observations on repeat poll | 0 |
| Event ID collision groups | 0 |
| New PostgreSQL observation rows | 44 |

The call counts reconcile: **105 = 44 active + 61 completed + 0 without ETA + 0 rejected call records**. Each active call in this response produced one accepted ETA observation. The adapter retains only selected Portnet ETA fields; raw API response bodies are not included in this report or committed.

This was a first poll, so it observed no repeat-poll duplicates. Idempotent replay and collision handling were verified separately with deterministic injected records in isolated tests; those cases are not represented as Portnet defects.
