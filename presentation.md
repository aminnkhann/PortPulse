# PortPulse presentation guide

Use this guide with [presentation.html](presentation.html). It follows the slides in order and gives you a simple story to tell. Aim for about 6–8 minutes; you can shorten it by using only the bold opening sentence and the main point on each slide.

## The one-sentence story

**PortPulse takes port-call ETA observations, checks their quality, keeps a traceable history, and makes the resulting data easier to trust.**

## Before you begin

- Open `presentation.html` in a browser. Use the arrow keys or the Previous/Next buttons.
- The project is an educational demonstration. Phase 1 uses synthetic data; Phase 2 uses one real source, Digitraffic Portnet V1.
- Say “implemented” for Phases 1 and 2. Say “planned” for dbt, Airflow, and AWS.
- If you show a live API report, call it a snapshot. Do not present its counts as a long-term reliability measure.

## Talk track

### Slide 1 — From ETA events to trusted port-call data

“I’m presenting PortPulse, a small data engineering project about port-call estimated arrival times. The project asks how we can turn changing observations into data that is checked, traceable, and useful. I’ll show the controlled workflow first, then the live-source workflow, and finish with the next steps.”

**Transition:** “First, why does this need a pipeline?”

### Slide 2 — An ETA is a changing observation

“An ETA is not a permanent fact. Records can arrive with missing fields, arrive again on a later poll, or revise an earlier ETA. If we simply overwrite a value, we lose the ability to explain where it came from. PortPulse keeps validation and quality outcomes visible.”

**Main point:** We need to know what arrived and why the system accepted or rejected it.

### Slide 3 — Build confidence in steps

“I split the project into steps. Phase 1 proves the event rules with a fixed synthetic fixture. Phase 2 applies validation and storage to Portnet V1 data. The next learning step is dbt for transformations. Phase 3 is planned for scheduling with Airflow and running the chosen components on AWS.”

**Main point:** Only the first two phases are implemented today.

### Slide 4 — A controlled first pipeline

“In Phase 1, JSON Lines events are validated, loaded into PostgreSQL, and reconciled using versioned rules. Invalid events are rejected with reasons. The accepted history can be replayed, and the same fixture produces the same golden result.”

**Main point:** Synthetic data makes the core behavior repeatable and easy to explain.

### Slide 5 — Keep the evidence behind the answer

“The pipeline distinguishes an exact repeat from a collision, where the same ID has conflicting content. A collided ID cannot change the trusted state. We also keep source update time, receipt time, and database ingestion time separate. These details help explain how the final answer was reached.”

**Main point:** The trusted result should be explainable from the records and rules behind it.

### Slide 6 — One supported live source

“Phase 2 connects to Digitraffic Portnet V1. The Python adapter validates the provider data and writes accepted observations, rejected records, and quality issues to dedicated PostgreSQL tables. Exact repeats and collisions are reported separately. This is the only supported live ingestion path in the current project.”

**Main point:** Phase 2 extends the data-quality approach to one real provider.

### Slide 7 — A revision is not automatically a duplicate

“For Portnet, an observation is identified by the port call, port area, and provider update timestamp. If the provider changes the ETA at a later update time, that is a new revision. Collection time and database ingestion time are recorded separately, so we can distinguish source behavior from our own processing delay.”

**Main point:** Identity and timestamps determine whether data is a revision, a repeat, or a conflict.

### Slide 8 — Separate live observations from test cases

“A documented live run received 125 records: 59 active calls, 65 completed calls, and one without an ETA. The 59 active calls yielded 59 accepted observations. Separately, six deterministic tests inject invalid or conflicting cases to check the safeguards. Those synthetic tests are not provider defects. One API poll is only a snapshot, so it cannot prove how the provider behaves over time.”

**Main point:** Be precise about what came from a live run and what came from controlled tests.

### Slide 9 — Run the deterministic path yourself

“The local demo starts PostgreSQL, ingests the fixture, reconciles it, and queries one port call. The expected trusted ETA is 15 January 2026 at 15:40 UTC. Because the inputs and rules are fixed, I can rerun the demo and explain the same result each time. Portnet can be run separately for a date supported by its API.”

**If you run it live:** Follow the setup and commands in the README. Use a disposable database for integration checks.

### Slide 10 — Trust comes from a visible process

“The main lesson is that trust comes from the steps around the data: validate it at the boundary, make identity and time rules explicit, keep a reason for rejected data, and separate test evidence from live evidence. This makes the result easier to inspect and extend.”

**Transition:** “That leads to the next learning steps.”

### Slide 11 — Next: dbt. Then Phase 3: Airflow and AWS.

“The next step is dbt: I can model the observations and quality tables into clean, tested datasets. After that, Phase 3 can schedule and monitor the pipeline with Airflow and place the selected components on AWS. Those tools are roadmap items, not part of the implementation I’ve shown today.”

**Closing sentence:** “PortPulse has a repeatable foundation; the next work is to transform it with dbt, then schedule and host it in Phase 3.”

## Quick answers if asked

**Why start with synthetic data?** It makes the important rules deterministic, replayable, and easier to verify before relying on a live provider.

**What does quarantine mean here?** A record that fails validation is preserved with a reason, but it does not become an accepted observation.

**What is an ID collision?** Two different payloads claim the same event identity. The pipeline reports the conflict and prevents the ambiguous identity from changing trusted state.

**Is the project using AIS or Digitraffic V2?** No. Those experiments are historical; the supported Phase 2 path is Portnet V1 only.

**Is dbt, Airflow, or AWS already set up?** No. dbt is the next planned step; Airflow and AWS are planned for Phase 3.

**Do the live counts prove reliability?** No. They describe a point-in-time run. Deterministic tests verify selected repeat and collision behavior separately.

## Details to keep straight

- Phase 1 fixture data is synthetic; Phase 2 observations come from Portnet V1.
- Provider update, API collection, and database ingestion timestamps have different meanings.
- A later ETA revision can be valid new data; an exact repeat is a duplicate; conflicting reuse of an identity is a collision.
- The current supported project has no scheduler, dashboard, dbt project, Airflow DAG, or AWS deployment.
- The cleanup PR has been merged, and no checks are currently waiting, as confirmed by the project owner.
