from topic_c_tokenization import apply_cdc, sanitize_event, tokenize


def test_tokenisation_is_deterministic_but_not_plaintext():
    event = {
        "trip_id": "t-1",
        "phone": "+84901234567",
        "national_id": "079123456789",
        "lat": 10.7769,
        "lon": 106.7009,
        "op": "UPSERT",
        "source_scn": 10,
    }
    clean = sanitize_event(event)
    assert tokenize(event["phone"]) == clean["phone_token"]
    assert event["phone"] not in clean.values()
    assert event["national_id"] not in clean.values()
    assert "phone" not in clean and "national_id" not in clean


def test_late_and_duplicate_cdc_events_do_not_overwrite_newer_state():
    events = [
        {"trip_id": "t-1", "status": "completed", "phone": "+84901234567",
         "national_id": "079123456789", "lat": 10.77, "lon": 106.70,
         "op": "UPSERT", "source_scn": 12},
        {"trip_id": "t-1", "status": "accepted", "phone": "+84901234567",
         "national_id": "079123456789", "lat": 10.77, "lon": 106.70,
         "op": "UPSERT", "source_scn": 11},
        {"trip_id": "t-1", "status": "completed", "phone": "+84901234567",
         "national_id": "079123456789", "lat": 10.77, "lon": 106.70,
         "op": "UPSERT", "source_scn": 12},
    ]
    state = apply_cdc(events)
    assert state["t-1"]["status"] == "completed"
    assert state["t-1"]["source_scn"] == 12
