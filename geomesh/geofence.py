"""
GeoMesh-Stream: Geofence Definitions, Ray-Casting & State Transition Engine.
Tracks entity transitions (ENTER, EXIT, DWELL) across Radial and Polygon geofences.
"""

from __future__ import annotations

import abc
import collections
import math
import uuid
from typing import Dict, List, Optional, Set

from geomesh.index import SpatialGridIndex
from geomesh.math import haversine_distance, point_in_polygon
from geomesh.types import (
    BoundingBox,
    GeofenceAlert,
    GeofenceAlreadyExistsError,
    GeofenceEvent,
    GeofenceNotFoundError,
    GeofenceType,
    InvalidPolygonError,
    Point,
)


class Geofence(abc.ABC):
    """Abstract base class for all geometric geofence perimeters."""

    def __init__(self, geofence_id: str, name: str, fence_type: GeofenceType) -> None:
        self.geofence_id = geofence_id
        self.name = name
        self.fence_type = fence_type

    @property
    @abc.abstractmethod
    def bounding_box(self) -> BoundingBox:
        """Axis-aligned bounding box bounding the perimeter."""

    @abc.abstractmethod
    def contains_point(self, point: Point) -> bool:
        """Check if geographic point is inside the perimeter."""


class RadialGeofence(Geofence):
    """Circular geofence defined by center coordinate and radius in meters."""

    def __init__(self, geofence_id: str, center: Point, radius_m: float, name: str = "") -> None:
        super().__init__(geofence_id, name or geofence_id, GeofenceType.RADIAL)
        if radius_m <= 0.0:
            raise ValueError(f"Radial geofence radius must be positive, got {radius_m}")
        self.center = center
        self.radius_m = radius_m

        # Compute tight bounding box
        d_lat = radius_m / 111_320.0
        cos_lat = math.cos(math.radians(center.lat))
        d_lon = radius_m / (111_320.0 * max(0.01, cos_lat))
        self._bbox = BoundingBox(
            min_lat=max(-90.0, center.lat - d_lat),
            min_lon=max(-180.0, center.lon - d_lon),
            max_lat=min(90.0, center.lat + d_lat),
            max_lon=min(180.0, center.lon + d_lon),
        )

    @property
    def bounding_box(self) -> BoundingBox:
        return self._bbox

    def contains_point(self, point: Point) -> bool:
        if not self._bbox.contains(point):
            return False
        return haversine_distance(self.center, point) <= self.radius_m


class PolygonGeofence(Geofence):
    """Arbitrary convex or concave polygon geofence defined by ordered vertices."""

    def __init__(self, geofence_id: str, vertices: List[Point], name: str = "") -> None:
        super().__init__(geofence_id, name or geofence_id, GeofenceType.POLYGON)
        if len(vertices) < 3:
            raise InvalidPolygonError(f"Polygon requires at least 3 vertices, got {len(vertices)}")

        self.vertices = list(vertices)

        # Compute bounding box
        lats = [v.lat for v in vertices]
        lons = [v.lon for v in vertices]
        self._bbox = BoundingBox(
            min_lat=min(lats),
            min_lon=min(lons),
            max_lat=max(lats),
            max_lon=max(lons),
        )

    @property
    def bounding_box(self) -> BoundingBox:
        return self._bbox

    def contains_point(self, point: Point) -> bool:
        if not self._bbox.contains(point):
            return False
        return point_in_polygon(point, self.vertices)


class GeofenceEngine:
    """
    High-throughput geofence evaluation and state-machine transition engine.
    Uses SpatialGridIndex to prune point containment checks from O(N) to O(1) candidates.
    """

    def __init__(self, cell_size_deg: float = 0.02) -> None:
        self.index = SpatialGridIndex(cell_size_deg=cell_size_deg)
        self._geofences: Dict[str, Geofence] = {}
        # entity_id -> {geofence_id -> enter_timestamp_ns}
        self._active_presence: Dict[str, Dict[str, int]] = collections.defaultdict(dict)

    def register_geofence(self, geofence: Geofence) -> None:
        """Register a geofence and index its bounding box."""
        if geofence.geofence_id in self._geofences:
            raise GeofenceAlreadyExistsError(f"Geofence '{geofence.geofence_id}' already registered.")

        self._geofences[geofence.geofence_id] = geofence
        self.index.insert(geofence.geofence_id, geofence.bounding_box)

    def unregister_geofence(self, geofence_id: str) -> None:
        """Unregister a geofence and remove from spatial index."""
        if geofence_id not in self._geofences:
            raise GeofenceNotFoundError(f"Geofence '{geofence_id}' not found.")

        del self._geofences[geofence_id]
        self.index.remove(geofence_id)

        # Clear from active presence
        for ent_fences in self._active_presence.values():
            ent_fences.pop(geofence_id, None)

    def get_geofence(self, geofence_id: str) -> Optional[Geofence]:
        return self._geofences.get(geofence_id)

    def count_geofences(self) -> int:
        return len(self._geofences)

    def evaluate_point(
        self,
        entity_id: str,
        point: Point,
        timestamp_ns: int,
    ) -> List[GeofenceAlert]:
        """
        Evaluate an entity coordinate against all registered geofences.
        Emits ENTER alerts upon crossing perimeter inwards, and EXIT alerts upon crossing outwards.
        """
        # Step 1: Spatial Grid Index candidate pruning
        candidates = self.index.query_point(point)

        # Previously active geofences for this entity
        current_active = self._active_presence[entity_id]
        newly_inside: Set[str] = set()
        alerts: List[GeofenceAlert] = []

        # Check candidate geofences
        for fence_id in candidates:
            fence = self._geofences.get(fence_id)
            if fence and fence.contains_point(point):
                newly_inside.add(fence_id)

        # Detect ENTER events
        for fence_id in newly_inside:
            if fence_id not in current_active:
                current_active[fence_id] = timestamp_ns
                alerts.append(
                    GeofenceAlert(
                        alert_id=f"alt-{uuid.uuid4().hex[:12]}",
                        entity_id=entity_id,
                        geofence_id=fence_id,
                        event=GeofenceEvent.ENTER,
                        timestamp_ns=timestamp_ns,
                        point=point,
                        dwell_duration_sec=0.0,
                    )
                )

        # Detect EXIT events (fences entity was in, but is no longer in)
        exited_fences = [f_id for f_id in current_active if f_id not in newly_inside]
        for fence_id in exited_fences:
            enter_ts = current_active.pop(fence_id)
            dwell_sec = max(0.0, (timestamp_ns - enter_ts) / 1_000_000_000.0)
            alerts.append(
                GeofenceAlert(
                    alert_id=f"alt-{uuid.uuid4().hex[:12]}",
                    entity_id=entity_id,
                    geofence_id=fence_id,
                    event=GeofenceEvent.EXIT,
                    timestamp_ns=timestamp_ns,
                    point=point,
                    dwell_duration_sec=dwell_sec,
                )
            )

        return alerts
