"""
GeoMesh Client SDK: Idiomatic high-performance client for the GeoMesh-Stream Spatial Engine.
Supports high-throughput GPS telemetry streaming, dynamic geofence management, proximity querying,
dead-reckoning position projection, and fleet forensic snapshots.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Union

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore


class GeoMeshClient:
    """
    Client for interacting with a live GeoMesh-Stream geospatial engine server.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        api_key: Optional[str] = None,
        timeout_seconds: float = 10.0,
        http_client: Optional[Any] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        if http_client is not None:
            self._client = http_client
        elif httpx is not None:
            self._client = httpx.Client(
                base_url=self.base_url,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        else:
            self._client = None

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def __enter__(self) -> GeoMeshClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def create_radial_geofence(
        self,
        geofence_id: str,
        lat: float,
        lon: float,
        radius_m: float,
        name: str = "",
    ) -> Dict[str, Any]:
        """Register a circular geofence with center and radius in meters."""
        payload = {
            "geofence_id": geofence_id,
            "lat": lat,
            "lon": lon,
            "radius_m": radius_m,
            "name": name or geofence_id,
        }
        resp = self._client.post("/v1/geofences/radial", json=payload)
        resp.raise_for_status()
        return resp.json()

    def create_polygon_geofence(
        self,
        geofence_id: str,
        vertices: List[Dict[str, float]],
        name: str = "",
    ) -> Dict[str, Any]:
        """Register an arbitrary polygon geofence with boundary vertices."""
        payload = {
            "geofence_id": geofence_id,
            "vertices": vertices,
            "name": name or geofence_id,
        }
        resp = self._client.post("/v1/geofences/polygon", json=payload)
        resp.raise_for_status()
        return resp.json()

    def ingest_telemetry(
        self,
        entity_id: str,
        lat: float,
        lon: float,
        speed_mps: Optional[float] = None,
        heading_deg: Optional[float] = None,
        timestamp_ns: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Ingest real-time GPS telemetry ping, updating kinematics and triggering geofence alerts."""
        payload = {
            "entity_id": entity_id,
            "lat": lat,
            "lon": lon,
            "speed_mps": speed_mps,
            "heading_deg": heading_deg,
            "timestamp_ns": timestamp_ns or time.time_ns(),
        }
        resp = self._client.post("/v1/telemetry", json=payload)
        resp.raise_for_status()
        return resp.json()

    def query_proximity(self, lat: float, lon: float, radius_m: float) -> Dict[str, Any]:
        """Query all active fleet entities within radius_m of a coordinate."""
        params = {"lat": lat, "lon": lon, "radius_m": radius_m}
        resp = self._client.get("/v1/proximity", params=params)
        resp.raise_for_status()
        return resp.json()

    def predict_position(self, entity_id: str, future_delta_sec: float = 30.0) -> Dict[str, Any]:
        """Extrapolate entity position using kinematic dead reckoning."""
        params = {"future_delta_sec": future_delta_sec}
        resp = self._client.get(f"/v1/predict/{entity_id}", params=params)
        resp.raise_for_status()
        return resp.json()

    def export_geofences(self) -> Dict[str, Any]:
        """Export all registered geofences for audit and disaster recovery."""
        resp = self._client.get("/v1/geofences/export")
        resp.raise_for_status()
        return resp.json()

    def snapshot_fleet(self) -> Dict[str, Any]:
        """Capture point-in-time state checkpoint of all tracked fleet vehicles."""
        resp = self._client.post("/v1/fleet/snapshot")
        resp.raise_for_status()
        return resp.json()

    def get_metrics(self) -> str:
        """Fetch Prometheus metrics."""
        resp = self._client.get("/metrics")
        resp.raise_for_status()
        return resp.text
