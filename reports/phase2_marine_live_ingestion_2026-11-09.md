> **Historical multi-source experiment:** These November 9 Portnet/AIS metrics describe the earlier marine branch. They are preserved as historical evidence and do not describe the Portnet-only deliverable. Any new Portnet-only run is reported separately. The earlier command and code are not part of this branch.

# Phase 2 real API ingestion report

Historical terminal output from the third same-date API poll against local PostgreSQL Docker on 2026-11-09. No test records were loaded. The command below repeats the live fetch-and-load process; current API responses may differ. The original raw API responses were not archived, so every historical source-record count cannot be reconstructed exactly.

Reproduce with:

```sh
python -m scripts.ingest_marine_etas --date 2026-11-09
```

## Pipeline health (historical run output)

| Metric | Result |
| --- | ---: |
| Portnet call records received | 292 |
| AIS records received | 980 |
| Accepted Portnet ETA observations/revisions | 142 |
| Accepted AIS comparison observations | 11 |
| Rejected source observations | 0 |
| Valid Portnet observations without a unique AIS match | 131 |
| Portnet observations missing matching fields (subset of unmatched) | 37 |
| AIS observations unmatched | 743 |
| AIS observations skipped because ETA was unavailable | 226 |
| Completed Portnet call records skipped | 147 |
| Portnet call records without ETA skipped | 4 |
| Active Portnet call records | 141 |
| Exact duplicate normalized event attempts in response (old metric) | 0 |
| Unchanged normalized observations on the third poll | 149 |
| Event ID collision groups | 0 |
| Calls with conflicting Portnet/AIS ETAs | 6 of 11 comparisons |

The 149 unchanged observations are repeated-poll observations, not defective provider data. Four other AIS observations had new provider timestamps between polls. The six conflicts are differences between real Portnet and AIS ETA values. No injected defects were included in this report.

## Counting correction and evidence

The original output mixed Portnet call-record counts with ETA-observation counts. The call-level categories reconcile: **292 calls = 147 completed + 4 without an ETA + 141 with an active ETA**. Accepted Portnet observations count ETA details, including revisions, so they need not equal active calls. PostgreSQL contains 142 accepted Portnet rows across 141 call IDs; `maritime:3411851` has two different `etaTimestamp` revisions for area `BG`. Thus 142 + 147 + 4 was not a valid partition.

The original `0` duplicate figure only counted exact duplicates among normalized accepted events. It did not compare every raw API record, including unmatched or skipped rows. The raw responses were not archived, so the historical number of identical Portnet call records and AIS records cannot be independently recovered. New runs report those response-level counts separately from duplicate normalized event attempts and repeat-poll observations.

The six ETA disagreements were checked against the saved `raw_event.raw_payload.source_payload` fields. The 11 selected pairs all have matching MMSI and destination values, and a unique Portnet call. AIS's encoded ETA has no year; the Portnet ETA supplies the year. The candidate match uses ETA month/day and the AIS API's UTC-date query window, not `portCallTimestamp` or event-time proximity. It supports candidate consistency but does not prove full vessel-to-call identity. The disagreement threshold is strictly greater than 30 minutes.

| Port call | Portnet ETA (UTC) | AIS ETA (UTC) | Difference |
| --- | --- | --- | ---: |
| `maritime:3411981` | 2026-11-10 23:00 | 2026-11-10 21:30 | 90 min |
| `maritime:3412013` | 2026-11-10 21:59 | 2026-11-10 19:00 | 179 min |
| `maritime:3412058` | 2026-11-11 00:00 | 2026-11-11 02:00 | 120 min |
| `maritime:3412151` | 2026-11-11 04:00 | 2026-11-11 06:00 | 120 min |
| `maritime:3412647` | 2026-11-10 08:00 | 2026-11-10 07:00 | 60 min |
| `maritime:3412831` | 2026-11-10 10:00 | 2026-11-10 12:00 | 120 min |

These are cross-source value disagreements, not invalid records. Priority 2 for Portnet and 1 for AIS remains a project demonstration assumption. Freshness is based on when the API observation was collected; an older Portnet `etaTimestamp` is not treated as late network delivery. The saved events do not include the complete raw response for unmatched/skipped records, so those portions of the historical poll cannot be reconstructed from PostgreSQL.
