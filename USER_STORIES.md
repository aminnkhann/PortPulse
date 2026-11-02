# User stories and acceptance criteria — PortPulse — Operational Event Integrity & Terminal Readiness

**Status:** Draft candidates for group refinement. The guide asks the team to define realistic functions; the mentor additionally requested user stories. User stories are not a separately named mandatory document in the guide.

**Delivery phases:** Phase 1 is Steps 01–06 (P0/MVP). Phase 2 is Stretch 1 (P1), including expanded API/scenarios, database integration checks and reporting. Phase 3 is Stretch 2 (P2), for optional platform and deployment work. Stretch work starts after Phase 1. P1 stories are optional and not part of the MVP gate.

**Story format:** As a [role], I want [behavior], so that [benefit]. Each criterion below should be verified by a small fixture or repeatable test.

## PP-US01 — P0

**Story:** As a **Operations analyst**, I want to **view the trusted current ETA and which event determined it** so that **I avoid using stale ETA information**.

**Traceability:** PP-FR04.

**Acceptance criteria:**

1. Given one valid ETA event for a port call, when the query is made, then the ETA, event ID, source and update time are returned.
2. Given a later valid update from the same authority, when the state is rebuilt, then the newer ETA is current and the earlier one remains in history.

**Implementation status:** Not started. **Owner:** Group to assign. **Reviewer:** Another group member.

## PP-US02 — P0

**Story:** As a **Data engineer**, I want to **exclude malformed and duplicated messages from trusted state** so that **bad inputs cannot silently change business results**.

**Traceability:** PP-FR02, PP-FR03.

**Acceptance criteria:**

1. Given an event missing `port_call_id`, when validation runs, then the original payload and a reason are retained outside trusted state.
2. Given the same `event_id` twice, when ingestion is repeated, then trusted event count does not increase twice.

**Implementation status:** Not started. **Owner:** Group to assign. **Reviewer:** Another group member.

## PP-US03 — P0

**Story:** As a **Operations analyst**, I want to **see a warning when ETA sources disagree** so that **I know the operational view requires investigation**.

**Traceability:** PP-FR06.

**Acceptance criteria:**

1. Given two valid equal-authority sources with materially different active ETA values, when state is queried, then `conflict_flag` and affected sources are returned.
2. Given an outdated source, when freshness threshold is exceeded, then the response shows a stale-source warning.

**Implementation status:** Not started. **Owner:** Group to assign. **Reviewer:** Another group member.

## PP-US04 — P0

**Story:** As a **Data engineer**, I want to **reconstruct state from a history containing a late event** so that **an out-of-order arrival does not corrupt current values**.

**Traceability:** PP-FR05, PP-FR07.

**Acceptance criteria:**

1. Given a newer event followed by an older event received late, when the history is replayed, then current ETA matches the documented ordering policy.
2. Given replay of exactly the same fixture, when rerun, then trusted state equals the first run.

**Implementation status:** Not started. **Owner:** Group to assign. **Reviewer:** Another group member.

## PP-US05 — P1

**Story:** As a **Platform support**, I want to **inspect reasons for validation failures** so that **I can investigate errors without reading every log**.

**Traceability:** PP-FR02.

**Acceptance criteria:**

1. Given rejected messages, when quality summary is requested, then rejected count and reason categories are visible.

**Implementation status:** Not started. **Owner:** Group to assign. **Reviewer:** Another group member.

## Story workflow

During planning, agree on P0 priority and ownership; before implementation, create one issue per P0 story. For each issue, add expected input fixture, output behavior, test name, file ownership and reviewer. No story is complete merely because code runs once: it must pass acceptance tests and be reviewed by another group member.
