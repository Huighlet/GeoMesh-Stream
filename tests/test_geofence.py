"""
Tests for Geofence definitions, perimeter containment, and GeofenceEngine transition events.
"""

import pytest
from geomesh.geofence import GeofenceEngine, PolygonGeofence, RadialGeofence
from geomesh.types import GeofenceAlreadyExistsError, GeofenceEvent, Point


def test_radial_geofence_containment():
    center = Point(lat=5.6037, lon=-0.1870)
    fence = RadialGeofence("airport-zone", center=center, radius_m=500.0)

    # Point ~100m away
    inside_pt = Point(lat=5.6042, lon=-0.1870)
    assert fence.contains_point(inside_pt) is True

    # Point ~2000m away
    outside_pt = Point(lat=5.6200, lon=-0.1870)
    assert fence.contains_point(outside_pt) is False


def test_polygon_geofence_containment():
    vertices = [
        Point(lat=10.0, lon=10.0),
        Point(lat=10.0, lon=20.0),
        Point(lat=20.0, lon=20.0),
        Point(lat=20.0, lon=10.0),
    ]
    poly = PolygonGeofence("custom-poly", vertices=vertices)

    assert poly.contains_point(Point(lat=15.0, lon=15.0)) is True
    assert poly.contains_point(Point(lat=25.0, lon=15.0)) is False


def test_geofence_engine_transition_lifecycle():
    engine = GeofenceEngine(cell_size_deg=0.05)
    center = Point(lat=5.6000, lon=-0.1800)
    fence = RadialGeofence("depot", center=center, radius_m=500.0)
    engine.register_geofence(fence)

    # 1. Entity starts outside (no alerts)
    p_out = Point(lat=5.6100, lon=-0.1800)  # ~1.1km away
    alerts_1 = engine.evaluate_point("truck-1", p_out, timestamp_ns=1_000_000_000)
    assert alerts_1 == []

    # 2. Entity moves INSIDE (ENTER alert emitted)
    p_in = Point(lat=5.6001, lon=-0.1800)  # ~11m from center
    alerts_2 = engine.evaluate_point("truck-1", p_in, timestamp_ns=2_000_000_000)
    assert len(alerts_2) == 1
    assert alerts_2[0].event == GeofenceEvent.ENTER
    assert alerts_2[0].geofence_id == "depot"
    assert alerts_2[0].entity_id == "truck-1"

    # 3. Entity stays inside (no new ENTER alert)
    p_in2 = Point(lat=5.6002, lon=-0.1800)
    alerts_3 = engine.evaluate_point("truck-1", p_in2, timestamp_ns=5_000_000_000)
    assert alerts_3 == []

    # 4. Entity leaves (EXIT alert emitted with dwell time = 8s - 2s = 6s)
    alerts_4 = engine.evaluate_point("truck-1", p_out, timestamp_ns=8_000_000_000)
    assert len(alerts_4) == 1
    assert alerts_4[0].event == GeofenceEvent.EXIT
    assert alerts_4[0].geofence_id == "depot"
    assert pytest.approx(alerts_4[0].dwell_duration_sec, abs=0.01) == 6.0


def test_duplicate_geofence_registration():
    engine = GeofenceEngine()
    f1 = RadialGeofence("zone-1", Point(0, 0), 100)
    f2 = RadialGeofence("zone-1", Point(0, 0), 200)

    engine.register_geofence(f1)
    with pytest.raises(GeofenceAlreadyExistsError):
        engine.register_geofence(f2)
