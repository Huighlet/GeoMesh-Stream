"""
GeoMesh-Stream: Spherical Trigonometry & Computational Geometry Primitives.
Provides microsecond Haversine distance, initial bearing, dead-reckoning projection,
and ray-casting point-in-polygon verification.
"""

from __future__ import annotations

import math
from typing import List, Tuple

from geomesh.types import Point

# Earth Mean Radius in meters (IUGG recommended value)
EARTH_RADIUS_METERS: float = 6_371_008.8


def haversine_distance(p1: Point, p2: Point) -> float:
    """
    Compute great-circle distance between two coordinates using the Haversine formula.
    Returns distance in meters.
    """
    phi1 = math.radians(p1.lat)
    phi2 = math.radians(p2.lat)
    delta_phi = math.radians(p2.lat - p1.lat)
    delta_lambda = math.radians(p2.lon - p1.lon)

    sin_half_phi = math.sin(delta_phi / 2.0)
    sin_half_lambda = math.sin(delta_lambda / 2.0)

    a = (sin_half_phi * sin_half_phi) + (
        math.cos(phi1) * math.cos(phi2) * (sin_half_lambda * sin_half_lambda)
    )
    # Clamp to avoid floating point domain errors
    a = min(1.0, max(0.0, a))
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    return EARTH_RADIUS_METERS * c


def initial_bearing(p1: Point, p2: Point) -> float:
    """
    Calculate initial forward azimuth / bearing from p1 to p2 in degrees [0.0, 360.0).
    """
    phi1 = math.radians(p1.lat)
    phi2 = math.radians(p2.lat)
    delta_lambda = math.radians(p2.lon - p1.lon)

    y = math.sin(delta_lambda) * math.cos(phi2)
    x = (math.cos(phi1) * math.sin(phi2)) - (
        math.sin(phi1) * math.cos(phi2) * math.cos(delta_lambda)
    )

    theta = math.atan2(y, x)
    bearing = (math.degrees(theta) + 360.0) % 360.0
    return bearing


def dead_reckoning_destination(p: Point, distance_m: float, bearing_deg: float) -> Point:
    """
    Project a new coordinate from a starting point, distance, and bearing.
    """
    delta = distance_m / EARTH_RADIUS_METERS
    theta = math.radians(bearing_deg)
    phi1 = math.radians(p.lat)
    lambda1 = math.radians(p.lon)

    sin_phi1 = math.sin(phi1)
    cos_phi1 = math.cos(phi1)
    sin_delta = math.sin(delta)
    cos_delta = math.cos(delta)

    sin_phi2 = (sin_phi1 * cos_delta) + (cos_phi1 * sin_delta * math.cos(theta))
    phi2 = math.asin(min(1.0, max(-1.0, sin_phi2)))

    y = math.sin(theta) * sin_delta * cos_phi1
    x = cos_delta - (sin_phi1 * math.sin(phi2))
    lambda2 = lambda1 + math.atan2(y, x)

    # Normalize longitude to [-180, 180]
    lat_deg = math.degrees(phi2)
    lon_deg = (math.degrees(lambda2) + 540.0) % 360.0 - 180.0

    return Point(lat=lat_deg, lon=lon_deg)


def point_in_polygon(point: Point, vertices: List[Point]) -> bool:
    """
    Ray-Casting Point-in-Polygon (PIP) Algorithm.
    Counts intersections of a horizontal ray extending eastward from the test point.
    Returns True if the point is strictly inside the polygon boundary.
    """
    num_v = len(vertices)
    if num_v < 3:
        return False

    inside = False
    x = point.lon
    y = point.lat

    p1 = vertices[0]
    for i in range(1, num_v + 1):
        p2 = vertices[i % num_v]

        # Check if point's y is between edge's y bounds
        if ((p1.lat > y) != (p2.lat > y)):
            # Compute x intersection of edge with horizontal line y
            # dx / dy * (y - p1.lat) + p1.lon
            denom = p2.lat - p1.lat
            if denom != 0.0:
                x_intersect = ((p2.lon - p1.lon) * (y - p1.lat) / denom) + p1.lon
                if x < x_intersect:
                    inside = not inside

        p1 = p2

    return inside
