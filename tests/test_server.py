"""
Tests for GeoMesh-Stream standalone REST server and Leaflet map dashboard.
"""

import pytest
from fastapi.testclient import TestClient

from geomesh import server


@pytest.fixture
def client() -> TestClient:
    return TestClient(server.app)


def test_health_endpoint(client: TestClient):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "HEALTHY"
    assert data["engine"] == "GeoMesh-Stream"


def test_leaflet_dashboard_html(client: TestClient):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "GeoMesh-Stream" in resp.text
    assert "leaflet" in resp.text


def test_create_radial_and_polygon_geofence(client: TestClient):
    # 1. Radial
    rad_payload = {
        "geofence_id": "accra-center",
        "name": "Accra Central",
        "lat": 5.6000,
        "lon": -0.1800,
        "radius_m": 1500.0,
    }
    resp1 = client.post("/v1/geofences/radial", json=rad_payload)
    assert resp1.status_code in (201, 409)

    # 2. Polygon
    poly_payload = {
        "geofence_id": "custom-terminal",
        "name": "Cargo Terminal",
        "vertices": [
            {"lat": 5.6100, "lon": -0.1900},
            {"lat": 5.6200, "lon": -0.1900},
            {"lat": 5.6200, "lon": -0.1800},
            {"lat": 5.6100, "lon": -0.1800},
        ],
    }
    resp2 = client.post("/v1/geofences/polygon", json=poly_payload)
    assert resp2.status_code in (201, 409)


def test_telemetry_ingestion_and_proximity(client: TestClient):
    ping_payload = {
        "entity_id": "courier-van-99",
        "lat": 5.6002,
        "lon": -0.1800,
        "speed_mps": 12.5,
        "heading_deg": 90.0,
    }
    resp = client.post("/v1/telemetry", json=ping_payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["entity_id"] == "courier-van-99"
    assert "kinematics" in data

    # Proximity query
    prox_resp = client.get("/v1/proximity?lat=5.6000&lon=-0.1800&radius_m=1000")
    assert prox_resp.status_code == 200
    prox_data = prox_resp.json()
    assert prox_data["match_count"] >= 1

    # Dead-reckoning prediction
    pred_resp = client.get("/v1/predict/courier-van-99?future_delta_sec=20")
    assert pred_resp.status_code == 200
    pred_data = pred_resp.json()
    assert "predicted_position" in pred_data
