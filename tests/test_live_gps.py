"""
Unit tests for the 1-Tap Mobile Browser GPS Ingestion & Departure Tracker (/launch).
"""

from fastapi.testclient import TestClient
from geomesh.server import app


def test_launch_page_html_endpoint():
    client = TestClient(app)
    resp = client.get("/launch")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "GH-MARSAR" in resp.text
    assert "DEPART BEACH" in resp.text
    assert "navigator.geolocation" in resp.text


def test_live_gps_ingestion_and_spatial_mesh():
    client = TestClient(app)
    payload = {
        "phone_number": "0243875318",
        "canoe_name": "Agbe Yeye",
        "landing_beach": "Keta Beach",
        "lat": 5.8250,
        "lon": 0.9150,
        "accuracy_m": 4.5,
        "speed_knots": 3.2,
        "heading_deg": 160.0,
        "is_sos": False,
    }
    resp = client.post("/v1/telecom/live-gps", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "INGESTED"
    assert data["entity_id"] == "canoe-+233243875318"
    assert data["lat"] == 5.8250
    assert data["lon"] == 0.9150

    # Verify vessel appears in fleet snapshot
    snap_resp = client.get("/v1/fleet/snapshot")
    assert snap_resp.status_code == 200
    snap = snap_resp.json()
    canoe = next((v for v in snap["vehicles"] if v["entity_id"] == "canoe-+233243875318"), None)
    assert canoe is not None
    assert canoe["position"]["lat"] == 5.8250
    assert canoe["position"]["lon"] == 0.9150


def test_live_gps_sos_emergency_triggers_sar():
    client = TestClient(app)
    payload = {
        "phone_number": "0243875318",
        "canoe_name": "Agbe Yeye",
        "landing_beach": "Keta Beach",
        "lat": 5.7500,
        "lon": 0.9500,
        "accuracy_m": 5.0,
        "speed_knots": 0.0,
        "heading_deg": 90.0,
        "is_sos": True,
    }
    resp = client.post("/v1/telecom/live-gps", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["sar_triggered"] is True
    assert data["incident_id"] is not None
    assert data["drift_datum"] is not None
    assert data["drift_datum"]["drift_distance_nm"] > 0
