"""tests.test_server_sdr: Test suite for SDR External SAR Intercept Server Endpoints."""

import pytest
from fastapi.testclient import TestClient

from geomesh.server import app


@pytest.fixture
def client():
    return TestClient(app)


def test_get_sdr_stations(client):
    response = client.get("/v1/sdr/stations")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 3
    station_ids = [s["station_id"] for s in data]
    assert "DRONE_KETA_SAR_1" in station_ids
    assert "STATION_KETA_MAST" in station_ids


def test_post_sdr_scan(client):
    payload = {
        "phone_number": "0243875318",
        "search_station_id": "DRONE_KETA_SAR_1",
        "band": "GSM_900",
        "force_simulated_detection": True,
    }
    response = client.post("/v1/sdr/scan", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "RF_FIX_ESTABLISHED"
    assert data["user_interaction_required"] is False
    assert data["weblink_clicked"] is False

    target = data["target"]
    assert target["phone_number"] == "+233243875318"
    assert target["status"] == "TARGET_ACQUIRED"
    assert target["external_method"] == "3GPP_RACH_RF_TIME_OF_FLIGHT"
    assert 5.70 <= target["lat"] <= 5.95
    assert 0.90 <= target["lon"] <= 1.20
    assert target["accuracy_m"] > 0


def test_post_sdr_triangulate(client):
    payload = {
        "phone_number": "0243875318",
    }
    response = client.post("/v1/sdr/triangulate", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["triangulated"] is True
    assert data["phone_number"] == "+233243875318"
    assert 5.70 <= data["lat"] <= 5.95
