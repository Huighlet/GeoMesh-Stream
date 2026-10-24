"""
End-to-end First-User Lifecycle & Functional Verification Test.
Tests all operational scenarios, edge cases, phone normalization,
CPA automatic robocall dispatch, SAR drift modeling, and state machine transitions.
"""

import pytest
from starlette.testclient import TestClient
from geomesh import server


@pytest.fixture
def client():
    return TestClient(server.app)


def test_first_user_complete_lifecycle(client: TestClient):
    # 1. Registration with local phone format and space
    reg_resp = client.post("/v1/telecom/register", json={
        "phone_number": "024 777 8899",
        "canoe_name": "Mawu Nye Agbe",
        "landing_beach": "Elmina",
        "language": "FANTE",
    })
    assert reg_resp.status_code == 201
    reg_data = reg_resp.json()
    assert reg_data["phone_number"] == "+233247778899"
    assert reg_data["canoe_name"] == "Mawu Nye Agbe"
    assert reg_data["status"] == "ON_SHORE"

    # 2. Passive seaward handover using landing beach name
    ev_resp = client.post("/v1/telecom/cell-event", json={
        "phone_number": "0247778899",  # Query with local format
        "tower_id": "Elmina",          # Tower mapped to Elmina
        "timing_advance": 8,           # 4.4 km offshore
    })
    assert ev_resp.status_code == 200
    ev_data = ev_resp.json()
    assert ev_data["status"] == "AT_SEA"
    assert "ELMINA" in ev_data["tower_id"].upper()
    assert ev_data["distance_offshore_km"] == 4.4
    canoe_pos = ev_data["derived_position"]

    # 3. Ingest converging AIS commercial tanker on collision course
    # Place tanker 1.4 NM south heading directly north
    tanker_lat = canoe_pos["lat"] - 0.023
    tanker_lon = canoe_pos["lon"]
    t_resp = client.post("/v1/telemetry", json={
        "entity_id": "MMSI-636019842",
        "lat": tanker_lat,
        "lon": tanker_lon,
        "speed_mps": 7.5,
        "heading_deg": 0.0,
    })
    assert t_resp.status_code == 200

    # 4. Verify automatic collision detection and native Fante voice robocall dispatch
    snap_resp = client.get("/v1/fleet/snapshot")
    assert snap_resp.status_code == 200
    snap_data = snap_resp.json()
    hazards = snap_data.get("collision_hazards", [])
    mmsi_hazards = [haz for haz in hazards if haz["threat_vessel"] == "MMSI-636019842"]
    assert len(mmsi_hazards) >= 1
    h = mmsi_hazards[0]
    assert h["threat_vessel"] == "MMSI-636019842"
    assert h["canoe_id"] == "canoe-+233247778899"
    assert h["cpa_nm"] <= 1.0
    assert 0.0 < h["tcpa_minutes"] <= 20.0
    assert h["robocall_dispatched"] is True
    assert "FANTE" in h["language"]
    assert "Nuakor" in h["voice_script"] or "kɛse" in h["voice_script"]

    # 5. Keypad phone SOS trigger (*920*44#)
    sos_resp = client.post("/v1/telecom/sos", json={
        "phone_number": "0247778899",
        "message": "ENGINE_LOST_HIGH_SEAS",
    })
    assert sos_resp.status_code == 200
    sos_data = sos_resp.json()
    assert sos_data["status"] == "SOS_DISTRESS_ACTIVATED"
    inc_id = sos_data["sar_incident_id"]
    assert inc_id is not None
    assert sos_data["projected_datum"]["lon"] > canoe_pos["lon"]  # Eastward drift towards Togo

    # 6. Verify SAR incidents listing
    sar_resp = client.get("/v1/sar/incidents")
    assert sar_resp.status_code == 200
    active_incidents = [i for i in sar_resp.json()["incidents"] if i["incident_id"] == inc_id]
    assert len(active_incidents) == 1
    assert active_incidents[0]["drift_distance_nm"] > 0.0

    # 7. Safe recovery: Resolve SAR incident
    resolve_resp = client.post(f"/v1/sar/resolve/{inc_id}")
    assert resolve_resp.status_code == 200
    assert resolve_resp.json()["status"] == "RESOLVED"

    # Verify canoe status reset to ON_SHORE
    subs_resp = client.get("/v1/telecom/subscribers")
    sub_record = [s for s in subs_resp.json()["subscribers"] if s["phone_number"] == "+233247778899"][0]
    assert sub_record["status"] == "ON_SHORE"

    # 8. Clean unenrollment and spatial index cleanup
    del_resp = client.delete("/v1/telecom/subscribers/+233247778899")
    assert del_resp.status_code == 200
    assert del_resp.json()["deleted"] is True

    # Confirm canoe entity completely removed from spatial mesh
    snap2 = client.get("/v1/fleet/snapshot").json()
    remaining = [v for v in snap2["vehicles"] if "247778899" in v["entity_id"]]
    assert len(remaining) == 0
