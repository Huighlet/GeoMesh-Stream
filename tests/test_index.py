"""
Tests for SpatialGridIndex cell hashing and candidate retrieval.
"""

import pytest
from geomesh.index import SpatialGridIndex
from geomesh.types import BoundingBox, Point


def test_spatial_grid_insert_and_query():
    grid = SpatialGridIndex(cell_size_deg=0.1)

    bbox_a = BoundingBox(min_lat=5.0, min_lon=-0.5, max_lat=5.2, max_lon=-0.3)
    bbox_b = BoundingBox(min_lat=6.0, min_lon=1.0, max_lat=6.2, max_lon=1.2)

    grid.insert("zone-a", bbox_a)
    grid.insert("zone-b", bbox_b)

    # Point in zone-a
    pt_a = Point(lat=5.1, lon=-0.4)
    res_a = grid.query_point(pt_a)
    assert "zone-a" in res_a
    assert "zone-b" not in res_a

    # Point in zone-b
    pt_b = Point(lat=6.1, lon=1.1)
    res_b = grid.query_point(pt_b)
    assert "zone-b" in res_b
    assert "zone-a" not in res_b

    # Point in vacant cell
    pt_vacant = Point(lat=0.0, lon=0.0)
    assert grid.query_point(pt_vacant) == set()


def test_spatial_grid_remove():
    grid = SpatialGridIndex(cell_size_deg=0.1)
    bbox = BoundingBox(min_lat=10.0, min_lon=20.0, max_lat=10.1, max_lon=20.1)

    grid.insert("item-1", bbox)
    assert "item-1" in grid.query_point(Point(lat=10.05, lon=20.05))

    grid.remove("item-1")
    assert "item-1" not in grid.query_point(Point(lat=10.05, lon=20.05))
    assert grid.total_cells == 0


def test_spatial_grid_radius_query():
    grid = SpatialGridIndex(cell_size_deg=0.05)
    center = Point(lat=5.6000, lon=-0.1800)

    # Insert items at varying distances
    near_bbox = BoundingBox(5.601, -0.181, 5.602, -0.180)  # ~150 meters
    far_bbox = BoundingBox(5.700, -0.180, 5.710, -0.170)   # ~11 km away

    grid.insert("near-item", near_bbox)
    grid.insert("far-item", far_bbox)

    # 1000m radius search
    candidates = grid.query_radius(center, radius_m=1000.0)
    assert "near-item" in candidates
    assert "far-item" not in candidates
