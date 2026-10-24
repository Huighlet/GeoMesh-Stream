"""
Tests for KinematicTracker velocity, bearing, acceleration, and dead-reckoning extrapolation.
"""

import pytest
from geomesh.kinematics import KinematicTracker
from geomesh.types import Point, TelemetryPing


def test_kinematic_velocity_and_bearing():
    tracker = KinematicTracker()

    # Initial ping at t=0
    p1 = TelemetryPing.create("car-1", lat=0.0, lon=0.0, timestamp_ns=1_000_000_000)
    s1 = tracker.update(p1)
    assert s1.velocity_mps == 0.0

    # Second ping 10 seconds later, 111.32 meters North
    # Approx 0.001 degree lat ~ 111.2 meters
    p2 = TelemetryPing.create("car-1", lat=0.001, lon=0.0, timestamp_ns=11_000_000_000)
    s2 = tracker.update(p2)

    # Velocity = ~111.2 m / 10 s = ~11.1 m/s
    assert 10.5 < s2.velocity_mps < 11.5
    # Heading North = 0.0 deg
    assert pytest.approx(s2.bearing_deg, abs=1.0) == 0.0
    # Acceleration = (11.1 - 0) / 10 = ~1.1 m/s^2
    assert 1.0 < s2.acceleration_mps2 < 1.2


def test_dead_reckoning_prediction():
    tracker = KinematicTracker()
    # Traveling North at 20 m/s
    p = TelemetryPing.create(
        "drone-9",
        lat=0.0,
        lon=0.0,
        speed_mps=20.0,
        heading_deg=0.0,
        timestamp_ns=1_000_000_000,
    )
    tracker.update(p)

    # Predict position 100 seconds ahead: distance = 20 * 100 = 2000 meters North
    pred = tracker.predict_position("drone-9", future_delta_sec=100.0)
    assert pred is not None
    assert pred.lat > 0.0  # Moved north
    assert pytest.approx(pred.lon, abs=1e-4) == 0.0
