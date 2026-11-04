# Step 01 — freeze the event contract

This is the first of six steps in **Phase 1 — MVP (P0)**. Finish Steps 01–06 before starting **Phase 2 — Stretch 1 (P1)**; **Phase 3 — Stretch 2 (P2)** follows Phase 2 only if time allows. See [progress.md](progress.md) for the scope of each phase.

Use these root-level project files to agree on the input shape and expected result **before writing the pipeline**. These are learning clues and starter templates, not completed project data.

## Coding sequence

1. **Read the contract:** review `docs/data_dictionary.md` and `contracts/port_call_event_v1.schema.json`.
2. **Review the rules:** `rules/eta_rules_v1.yaml` records the v1 simulation choices from the PRD. Confirm them with your group before changing or calling them approved.
3. **Generate the fixture:** run `python scripts/generate_fixture.py`. It deterministically writes about 30 synthetic event occurrences to `fixtures/port_call_v1.jsonl`.
4. **Review the golden answer:** `fixtures/expected_output.json` is the hand-worked expected result. Compare it with the generated input and rules; change it only when you intentionally revise the contract.
5. **Start coding in this order:** parse JSONL → validate schema/fields → group by event ID and detect exact duplicates/collisions → choose latest eligible event per source → evaluate freshness → choose trusted source → calculate conflict/delay/out-of-order issues → compare canonical output with the golden file.
6. **Keep scope small:** first make one valid event produce one trusted ETA. Add each edge case from the fixture one at a time. Leave Kafka, API, dashboard, and cloud work until this slice is correct.

## Done when

- The generator creates 31 JSON event occurrences deterministically; the invalid-schema case is valid JSON with an unsupported version.
- The fixture includes duplicates, an ID collision, two late out-of-order events, a delayed-but-newer event, a conflict, and a stale source.
- Rules are explicit and versioned; group sign-off is recorded before G1 is called approved.
- The golden output is hand-reviewed and states the fixed evaluation time.
- A teammate can explain why the expected source and event win.

## Important

The fixture is generated synthetic data. The golden output is a manually specified expectation, not output calculated by the production reconciliation code. Keep them independent so later tests can catch mistakes.
