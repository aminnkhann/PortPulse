# Portnet-only injected reliability test report

All failure cases in `tests/test_portnet_reliability_injected.py` use deterministic synthetic records. They are isolated tests, not observations from Portnet and not claims of provider defects.

Run with PostgreSQL enabled:

```sh
PORTPULSE_TEST_DATABASE_URL="${PORTPULSE_TEST_DATABASE_URL:?set to a disposable PostgreSQL test database}" \
  python -m unittest discover -s tests -p test_portnet_reliability_injected.py -v
```

The isolated run passed all 6 tests: invalid call and ETA quarantine, ambiguous area rejection, repeat-poll idempotency, and event-ID collision handling. PostgreSQL cases ran in temporary schemas and the records were synthetic.
