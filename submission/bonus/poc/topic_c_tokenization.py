# %% [markdown]
# # Topic C PoC — PII tokenisation + late CDC protection
#
# This is a deliberately small, dependency-free spike. It demonstrates the
# two contracts the ride-hailing design cannot compromise on:
#
# 1. plaintext phone/identity/GPS values do not enter readable Bronze;
# 2. a late or duplicate CDC event cannot overwrite a newer source SCN.
#
# Production would call an HSM/KMS-backed token service. This PoC uses HMAC
# with a demo key only to make the deterministic-token contract executable.

# %%
from __future__ import annotations

import hashlib
import hmac
from typing import Any, Iterable


TOKEN_KEY = b"demo-only-topic-c-key-v1"


def tokenize(value: str, key: bytes = TOKEN_KEY) -> str:
    """Return a deterministic, non-reversible token for an identifier."""
    digest = hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"tok_v1_{digest[:24]}"


def coarse_geohash(lat: float, lon: float, cell_size: float = 0.01) -> str:
    """Reduce GPS precision to an analyst-safe grid cell."""
    lat_cell = int(lat // cell_size)
    lon_cell = int(lon // cell_size)
    return f"geo_{lat_cell}_{lon_cell}"


def sanitize_event(event: dict[str, Any]) -> dict[str, Any]:
    """Convert one CDC envelope to the readable Bronze contract."""
    clean = {
        "trip_id": event["trip_id"],
        "status": event.get("status"),
        "op": event["op"],
        "source_scn": int(event["source_scn"]),
        "phone_token": tokenize(event["phone"]),
        "national_id_token": tokenize(event["national_id"]),
        "gps_geohash": coarse_geohash(event["lat"], event["lon"]),
    }
    # Canary invariant: source PII must never survive sanitisation.
    assert "phone" not in clean and "national_id" not in clean
    assert "lat" not in clean and "lon" not in clean
    return clean


def apply_cdc(events: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Apply CDC in source-SCN order without requiring arrival order.

    Equal or older SCNs are ignored. A DELETE removes the current projection;
    the production Silver table would also retain the tombstone in its SCD2
    history and emit it through Delta Change Data Feed.
    """
    current: dict[str, dict[str, Any]] = {}
    for event in events:
        trip_id = event["trip_id"]
        incoming_scn = int(event["source_scn"])
        existing = current.get(trip_id)
        if existing and incoming_scn <= existing["source_scn"]:
            continue
        if event["op"] == "DELETE":
            current.pop(trip_id, None)
            continue
        current[trip_id] = sanitize_event(event)
    return current


# %% [markdown]
# ## Executable contract

# %%
events = [
    {"trip_id": "trip-001", "status": "accepted", "phone": "+84901234567",
     "national_id": "079123456789", "lat": 10.7769, "lon": 106.7009,
     "op": "UPSERT", "source_scn": 100},
    {"trip_id": "trip-001", "status": "completed", "phone": "+84901234567",
     "national_id": "079123456789", "lat": 10.7769, "lon": 106.7009,
     "op": "UPSERT", "source_scn": 102},
    # Arrives late after SCN 102; must not roll status back to accepted.
    {"trip_id": "trip-001", "status": "accepted", "phone": "+84901234567",
     "national_id": "079123456789", "lat": 10.7769, "lon": 106.7009,
     "op": "UPSERT", "source_scn": 101},
    # Same SCN as the current record; duplicate must be ignored.
    {"trip_id": "trip-001", "status": "completed", "phone": "+84901234567",
     "national_id": "079123456789", "lat": 10.7769, "lon": 106.7009,
     "op": "UPSERT", "source_scn": 102},
    {"trip_id": "trip-002", "status": "accepted", "phone": "+84909876543",
     "national_id": "079987654321", "lat": 21.0278, "lon": 105.8342,
     "op": "UPSERT", "source_scn": 200},
    {"trip_id": "trip-002", "status": "cancelled", "phone": "+84909876543",
     "national_id": "079987654321", "lat": 21.0278, "lon": 105.8342,
     "op": "DELETE", "source_scn": 201},
]

state = apply_cdc(events)
assert state["trip-001"]["status"] == "completed"
assert state["trip-001"]["source_scn"] == 102
assert "trip-002" not in state
assert state["trip-001"]["phone_token"].startswith("tok_v1_")
assert "+84901234567" not in repr(state)
assert "079123456789" not in repr(state)

print("final current trips:", sorted(state))
print("trip-001 status:", state["trip-001"]["status"])
print("trip-001 token:", state["trip-001"]["phone_token"])
print("late/duplicate events ignored: PASS")
print("plaintext PII canary: PASS")
