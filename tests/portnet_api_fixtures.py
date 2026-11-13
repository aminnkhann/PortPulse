"""Deterministic Portnet-shaped responses for isolated reliability tests."""


def generate_portnet_response():
    revision = {
        "portAreaCode": "VUOS",
        "eta": "2026-11-10T13:00:00Z",
        "etaTimestamp": "2026-11-10T08:00:00Z",
        "etaSource": "PORT",
    }
    return {"portCalls": [
        {
            "portCallId": 1001,
            "portAreaDetails": [
                {"portAreaCode": "VUOS", "eta": "2026-11-10T12:00:00Z",
                 "etaTimestamp": "2026-11-10T07:00:00Z", "etaSource": "PORT"},
                revision,
                {"portAreaCode": "VUOS", "eta": "2026-11-10T12:00:00Z",
                 "etaTimestamp": "2026-11-10T07:00:00Z", "etaSource": "PORT"},
            ],
        },
        {
            "portCallId": 2002,
            "portAreaDetails": [
                {"portAreaCode": "KATA", "eta": "2026-11-10T13:30:00Z",
                 "etaTimestamp": "2026-11-10T07:30:00Z", "etaSource": "PORT"},
                {"portAreaCode": "KATA", "eta": "2026-11-10T14:00:00Z",
                 "etaTimestamp": "2026-11-10T09:00:00Z", "etaSource": "PORT"},
                {"portAreaCode": "KATA", "eta": "2026-11-10T14:30:00Z",
                 "etaTimestamp": "2026-11-10T09:00:00Z", "etaSource": "PORT"},
            ],
        },
        {"portCallId": 3003, "portAreaDetails": [
            {"portAreaCode": "VUOS", "eta": "2026-11-10T15:00:00Z",
             "etaTimestamp": "2026-11-10T08:00:00Z", "etaSource": "PORT"},
        ]},
        {"portCallId": 0, "portAreaDetails": []},
        {"portCallId": 4004, "portAreaDetails": [
            {"portAreaCode": "VUOS", "eta": "not-a-timestamp",
             "etaTimestamp": "2026-11-10T07:00:00Z"},
        ]},
        {"portCallId": 5005, "portAreaDetails": []},
    ]}
