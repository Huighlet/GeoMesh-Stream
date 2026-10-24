"""
Enterprise Platform Tests for GeoMesh-Stream.
Tests Client SDK, Rate Limiting, Prometheus Metrics, Geofence Manifest Export, and Fleet State Checkpoint Snapshots.
"""

import pytest
from starlette.testclient import TestClient
from geomesh.server import app
from geomesh.client import GeoMeshClient


@pytest.fixture
def test_client():
    return TestClient(app)


@pytest.fixture
def sdk_client(test_client):
    return GeoMeshClient(
        base_url="http://testserver",
        api_key="geomesh_test_key_xyz",
        http_client=test_client,
    )


def test_prometheus_metrics_endpoint(test_client):
    resp = test_client.get("/metrics")
    assert resp.status_code == 200
    assert "geomesh_geofences_total" in resp.text
    assert "geomesh_tracked_entities_total" in resp.text
    assert "geomesh_spatial_cells_total" in resp.text
    assert "geomesh_telemetry_pings_total" in resp.text


def test_rate_limiter_headers(test_client):
    resp = test_client.get("/health")
    assert resp.status_code == 200
    assert "X-RateLimit-Limit" in resp.headers
    assert "X-RateLimit-Remaining" in resp.headers
    assert resp.headers["X-RateLimit-Limit"] == "5000"


def test_geofences_export_manifest(sdk_client):
    # Register radial geofence
    sdk_client.create_radial_geofence(
        geofence_id="test-export-zone",
        lat=5.6037,
        lon=-0.1870,
        radius_m=750.0,
        name="Test Export Zone",
    )

    manifest = sdk_client.export_geofences()
    assert "geofences" in manifest
    assert manifest["total_geofences"] >= 1
    found = any(f["geofence_id"] == "test-export-zone" for f in manifest["geofences"])
    assert found is True


def test_fleet_state_snapshot(sdk_client):
    # Ingest telemetry for a couple vehicles
    sdk_client.ingest_telemetry(
        entity_id="truck-snap-01",
        lat=5.6040,
        lon=-0.1860,
        speed_mps=15.0,
        heading_deg=90.0,
    )
    sdk_client.ingest_telemetry(
        entity_id="truck-snap-02",
        lat=5.6050,
        lon=-0.1850,
        speed_mps=12.0,
        heading_deg=180.0,
    )

    snapshot = sdk_client.snapshot_fleet()
    assert snapshot["status"] == "FLEET_SNAPSHOT_COMMITTED"
    assert snapshot["total_tracked_vehicles"] >= 2
    tracked_ids = [v["entity_id"] for v in snapshot["vehicles"]]
    assert "truck-snap-01" in tracked_ids
    assert "truck-snap-02" in tracked_ids


def test_client_sdk_end_to_end(sdk_client):
    # 1. Polygon Geofence Creation
    verts = [
        {"lat": 5.6000, "lon": -0.1900},
        {"lat": 5.6100, "lon": -0.1900},
        {"lat": 5.6100, "lon": -0.1800},
        {"lat": 5.6000, "lon": -0.1800},
    ]
    poly = sdk_client.create_polygon_geofence(
        geofence_id="sdk-poly-test",
        vertices=verts,
        name="SDK Polygon",
    )
    assert poly["geofence_id"] == "sdk-poly-test"
    assert poly["vertex_count"] == 4

    # 2. Telemetry Ingest & Kinematics
    ingest = sdk_client.ingest_telemetry(
        entity_id="van-sdk-01",
        lat=5.6050,
        lon=-0.1850,
        speed_mps=20.0,
        heading_deg=45.0,
    )
    assert ingest["entity_id"] == "van-sdk-01"
    assert "kinematics" in ingest

    # 3. Proximity Query
    prox = sdk_client.query_proximity(lat=5.6050, lon=-0.1850, radius_m=500.0)
    assert prox["match_count"] >= 1
    assert any(e["entity_id"] == "van-sdk-01" for e in prox["entities"])

    # 4. Dead-Reckoning Prediction
    pred = sdk_client.predict_position(entity_id="van-sdk-01", future_delta_sec=15.0)
    assert pred["entity_id"] == "van-sdk-01"
    assert "predicted_position" in pred


def test_maritime_collision_risk_cpa(sdk_client):
    # Ingest two converging vessels
    sdk_client.ingest_telemetry(
        entity_id="tanker-01",
        lat=5.7800,
        lon=0.9200,
        speed_mps=8.0,
        heading_deg=240.0,
    )
    sdk_client.ingest_telemetry(
        entity_id="canoe-01",
        lat=5.7750,
        lon=0.9100,
        speed_mps=2.0,
        heading_deg=60.0,
    )

    cpa = sdk_client.calculate_collision_risk("tanker-01", "canoe-01")
    assert "cpa_meters" in cpa
    assert "tcpa_seconds" in cpa
    assert "cpa_nautical_miles" in cpa
    assert "risk_level" in cpa
