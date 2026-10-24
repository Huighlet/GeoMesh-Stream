"""
GeoMesh-Stream: Unified Streaming Geospatial Engine.
Coordinates telemetry ingestion, spatial grid indexing, kinematic tracking,
and real-time geofence breach detection.
"""

from __future__ import annotations

import collections
import threading
from typing import Dict, List, Optional, Tuple

from geomesh.geofence import Geofence, GeofenceEngine
from geomesh.index import SpatialGridIndex
from geomesh.kinematics import KinematicTracker
from geomesh.math import haversine_distance
from geomesh.types import (
    BoundingBox,
    GeofenceAlert,
    KinematicState,
    Point,
    TelemetryPing,
)


class GeoMeshEngine:
    """
    Unified real-time streaming geospatial engine.
    Processes high-throughput GPS telemetry streams, indexes moving assets in a spatial mesh,
    and dispatches low-latency geofence breach events.
    """

    def __init__(self, cell_size_deg: float = 0.02) -> None:
        self._lock = threading.RLock()
        self.geofences = GeofenceEngine(cell_size_deg=cell_size_deg)
        self.kinematics = KinematicTracker()
        self.entity_index = SpatialGridIndex(cell_size_deg=cell_size_deg)
        # entity_id -> Point
        self._entity_positions: Dict[str, Point] = {}

    def register_geofence(self, geofence: Geofence) -> None:
        """Register a new geofence."""
        with self._lock:
            self.geofences.register_geofence(geofence)

    def unregister_geofence(self, geofence_id: str) -> None:
        """Unregister an existing geofence."""
        with self._lock:
            self.geofences.unregister_geofence(geofence_id)

    def ingest_telemetry(self, ping: TelemetryPing) -> List[GeofenceAlert]:
        """
        Process an incoming telemetry ping:
        1. Updates kinematic state (velocity, heading, acceleration).
        2. Updates entity spatial index.
        3. Evaluates intersecting geofences and emits ENTER/EXIT alerts.
        """
        with self._lock:
            # Update kinematics
            self.kinematics.update(ping)

            # Update entity position index
            self._entity_positions[ping.entity_id] = ping.point
            # Single-point bounding box for entity indexing
            p_bbox = BoundingBox(
                min_lat=ping.point.lat,
                min_lon=ping.point.lon,
                max_lat=ping.point.lat,
                max_lon=ping.point.lon,
            )
            self.entity_index.insert(ping.entity_id, p_bbox)

            # Evaluate geofences
            alerts = self.geofences.evaluate_point(
                entity_id=ping.entity_id,
                point=ping.point,
                timestamp_ns=ping.timestamp_ns,
            )
            return alerts

    def query_radius(
        self,
        center: Point,
        radius_m: float,
    ) -> List[Tuple[str, Point, float]]:
        """
        Query all active entities within a given radius in meters.
        Returns list of (entity_id, Point, distance_meters) sorted by distance.
        """
        with self._lock:
            candidate_ids = self.entity_index.query_radius(center, radius_m)
            results: List[Tuple[str, Point, float]] = []

            for ent_id in candidate_ids:
                pt = self._entity_positions.get(ent_id)
                if not pt:
                    continue
                d = haversine_distance(center, pt)
                if d <= radius_m:
                    results.append((ent_id, pt, d))

            results.sort(key=lambda x: x[2])
            return results

    def get_kinematic_state(self, entity_id: str) -> Optional[KinematicState]:
        """Retrieve latest kinematic state of an asset."""
        with self._lock:
            return self.kinematics.get_state(entity_id)

    def predict_position(self, entity_id: str, future_delta_sec: float) -> Optional[Point]:
        """Predict future coordinate using dead-reckoning extrapolation."""
        with self._lock:
            return self.kinematics.predict_position(entity_id, future_delta_sec)
