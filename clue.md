# Phase 1 Fixture Guide

Phase 1 is implemented. This note remains as a short guide to the deterministic synthetic acceptance example.

1. Review the event fields in docs/data_dictionary.md and the generated contract in contracts/port_call_event_v1.schema.json.
2. Review the fixed simulation assumptions in rules/eta_rules_v1.yaml.
3. Run python -m scripts.generate_fixture to reproduce fixtures/port_call_v1.jsonl.
4. Compare fixture behavior with the hand-reviewed fixtures/expected_output.json.
5. Run the validator, PostgreSQL loader, reconciler, and query CLI as documented in README.md.

The fixture contains synthetic records only. Keep the expected result independent from the production reconciliation code so tests can catch mistakes.
