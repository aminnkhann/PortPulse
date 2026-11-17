# Synthetic maritime ETA dataset

`scripts/generate_synthetic_eta.py` creates standalone fictional JSONL records
using PortPulse's existing reconciliation event contract (`PortCallEvent`). It
covers a fixed 30-day UTC event window beginning 2026-01-01, uses seed 42 by
default, emits 2–5 valid ETA updates per fictional port call, and writes to
`fixtures/synthetic_eta_v1.jsonl`. It never reads or writes Portnet data.

The generated stream includes ETA revisions, delayed delivery, updates that
arrive after a newer update, duplicate deliveries, and invalid schema-version
events. Valid events use the existing normalized event fields. Invalid events
are included as rejected-input examples; they must not be treated as trusted
state. The printed summary reports the update and occurrence counts and each
scenario category.

Generate a small five-call dataset for a quick demonstration:

```bash
python -m scripts.generate_synthetic_eta --port-calls 5
```

Generate the full 1,000-call demonstration dataset:

```bash
python -m scripts.generate_synthetic_eta --port-calls 1000
```

Use `--seed` to reproduce a different deterministic dataset. The generator
rewrites only `fixtures/synthetic_eta_v1.jsonl`; it does not load the events
into PostgreSQL, power Metabase or CDC dashboards, or alter the existing
fixture, Portnet ingestion, or CDC demo. The current event contract does not
include port-area codes.

## Next integration steps

Integrate the JSONL events through a separate PostgreSQL loading path, extend
the event contract with a port-area code, and then connect the synthetic data
to analytics, Metabase, or CDC views. Those steps are outside this generator's
current scope.
