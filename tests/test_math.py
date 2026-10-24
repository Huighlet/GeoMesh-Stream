"""
Tests for spherical trigonometry and computational geometry functions.
"""

import math
import pytest
from geomesh.math import (
    dead_reckoning_destination,
    haversine_distance,
    initial_bearing,
    point_in_polygon,
)
from geomesh.types import Point


def test_haversine_known_distances():
    # Accra to Kumasi: ~200 km (within 1% margin)
    accra = Point(lat=5.6037, lon=-0.1870)
    kumasi = Point(lat=6.6885, lon=-1.6244)
    dist = haversine_distance(accra, kumasi)
    assert 195_000 < dist < 205_000

    # Same point distance is zero
    assert haversine_distance(accra, accra) == 0.0


def test_initial_bearing():
    origin = Point(lat=0.0, lon=0.0)
    north = Point(lat=10.0, lon=0.0)
    east = Point(lat=0.0, lon=10.0)
    south = Point(lat=-10.0, lon=0.0)
    west = Point(lat=0.0, lon=-10.0)

    assert pytest.approx(initial_bearing(origin, north), abs=1e-3) == 0.0
    assert pytest.approx(initial_bearing(origin, east), abs=1e-3) == 90.0
    assert pytest.approx(initial_bearing(origin, south), abs=1e-3) == 180.0
    assert pytest.approx(initial_bearing(origin, west), abs=1e-3) == 270.0


def test_dead_reckoning_destination():
    start = Point(lat=0.0, lon=0.0)
    # Travel 111.32 km directly north (approx 1 degree lat)
    dest = dead_reckoning_destination(start, distance_m=111_195.0, bearing_deg=0.0)
    assert pytest.approx(dest.lat, abs=0.05) == 1.0
    assert pytest.approx(dest.lon, abs=0.01) == 0.0


def test_point_in_polygon_convex():
    # Square from (0,0) to (10,10)
    poly = [
        Point(lat=0.0, lon=0.0),
        Point(lat=10.0, lon=0.0),
        Point(lat=10.0, lon=10.0),
        Point(lat=0.0, lon=10.0),
    ]

    inside_pt = Point(lat=5.0, lon=5.0)
    outside_pt = Point(lat=15.0, lon=5.0)

    assert point_in_polygon(inside_pt, poly) is True
    assert point_in_polygon(outside_pt, poly) is False


def test_point_in_polygon_concave():
    # L-shaped polygon
    poly = [
        Point(lat=0.0, lon=0.0),
        Point(lat=10.0, lon=0.0),
        Point(lat=10.0, lon=5.0),
        Point(lat=5.0, lon=5.0),
        Point(lat=5.0, lon=10.0),
        Point(lat=0.0, lon=10.0),
    ]

    # Inside the vertical leg
    assert point_in_polygon(Point(lat=8.0, lon=2.0), poly) is True
    # Inside the horizontal leg
    assert point_in_polygon(Point(lat=2.0, lon=8.0), poly) is True
    # In the cut-out corner (outside)
    assert point_in_polygon(Point(lat=8.0, lon=8.0), poly) is False
