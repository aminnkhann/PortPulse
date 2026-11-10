# Portnet-only live ingestion verification — rebuilt branch, 2026-11-10

This is a separate live Portnet V1 run made while verifying `feat/phase2-portnet-primary` against the latest `origin/main`. It does not replace the earlier [November 10 Portnet-only report](phase2_portnet_only_live_ingestion_2026-11-10.md), and it is not the [November 9 Portnet/AIS experiment](phase2_marine_live_ingestion_2026-11-09.md). No synthetic failures were included; see the separate [injected reliability report](phase2_portnet_injected_reliability_tests.md).

The run used an isolated disposable PostgreSQL database. Its data volume was removed after verification. No full API response was saved in the report or repository.

## Run metrics

| Metric | Result |
| --- | ---: |
| Portnet V1 call records received | 125 |
| Active Portnet calls | 59 |
| Completed calls skipped | 65 |
| Calls without ETA skipped | 1 |
| Rejected call records | 0 |
| Rejected ETA details | 0 |
| Accepted ETA observations | 59 |
| Duplicate call records in response | 0 |
| Duplicate normalized event attempts | 0 |
| Unchanged observations on repeat poll | 0 |
| Event ID collision groups | 0 |
| New PostgreSQL observation rows | 59 |

Call counts reconcile: **125 = 59 active + 65 completed + 1 without ETA + 0 rejected calls**. Each active call yielded one accepted observation in this response. The zero duplicate and collision counts are observations for this single poll; deterministic replay and collision behavior are separately exercised by synthetic tests.
