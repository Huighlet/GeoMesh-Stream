"""
Tests for unified GeoMeshEngine telemetry ingestion, proximity radius queries, and alerts.
"""

import pytest
from geomesh.engine import GeoMeshEngine
from geomesh.geofence import RadialGeofence
from geomesh.types import GeofenceEvent, Point, TelemetryPing


def test_engine_telemetry_and_alerts():
    engine = GeoMeshEngine(cell_size_deg=0.05)
    fence = RadialGeofence("terminal", center=Point(5.6000, -0.1800), radius_m=300.0)
    engine.register_geofence(fence)

    # Asset moves into terminal
    ping_in = TelemetryPing.create("bus-12", lat=5.6001, lon=-0.1800, timestamp_ns=1_000_000_000)
    alerts = engine.ingest_telemetry(ping_in)

    assert len(alerts) == 1
    assert alerts[0].event == GeofenceEvent.ENTER
    assert alerts[0].entity_id == "bus-12"

    # Verify kinematic state tracked
    state = engine.get_kinematic_state("bus-12")
    assert state is not None
    assert state.last_point.lat == 5.6001


def test_engine_query_radius():
    engine = GeoMeshEngine(cell_size_deg=0.05)

    # Ingest multiple entities around Accra
    center = Point(lat=5.6000, lon=-0.1800)
    # Asset 1 ~100m away
    p1 = TelemetryPing.create("v1", lat=5.6008, lon=-0.1800, timestamp_ns=1_000_000_000)
    # Asset 2 ~500m away
    p2 = TelemetryPing.create("v2", lat=5.6045, lon=-0.1800, timestamp_ns=1_000_000_000)
    # Asset 3 ~5km away
    p3 = TelemetryPing.create("v3", lat=5.6450, lon=-0.1800, timestamp_ns=1_000_000_000)

    engine.ingest_telemetry(p1)
    engine.ingest_telemetry(p2)
    engine.ingest_telemetry(p3)

    # Query 1km radius
    nearby = engine.query_radius(center, radius_m=1000.0)
    assert len(nearby) == 2
    # Verify sorted by distance
    assert nearby[0][0] == "v1"
    assert nearby[1][0] == "v2"
    assert nearby[0][2] < nearby[1][2]
