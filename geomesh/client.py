"""
GeoMesh Client SDK: Idiomatic high-performance client for the GeoMesh-Stream Spatial Engine.
Supports high-throughput GPS telemetry streaming, NMEA 0183 AIS radio ingestion,
carrier webhooks, Search and Rescue (SAR) incident handling, and fleet forensic snapshots.
"""

from __future__ import annotations

import hashlib
import hmac
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

    def calculate_collision_risk(self, vessel_1: str, vessel_2: str) -> Dict[str, Any]:
        """Calculate Closest Point of Approach (CPA in NM) and Time to CPA (TCPA in mins) between two vessels."""
        params = {"vessel_1": vessel_1, "vessel_2": vessel_2}
        resp = self._client.get("/v1/maritime/cpa", params=params)
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

    # -------------------------------------------------------------------------
    # Production Maritime & Telecom Extensions
    # -------------------------------------------------------------------------

    def register_subscriber(
        self,
        phone_number: str,
        canoe_name: str,
        landing_beach: str = "Keta Beach",
        language: str = "EWE",
    ) -> Dict[str, Any]:
        """Enroll an artisanal fisherman into the zero-touch telecom safety registry."""
        payload = {
            "phone_number": phone_number,
            "canoe_name": canoe_name,
            "landing_beach": landing_beach,
            "language": language,
        }
        resp = self._client.post("/v1/telecom/register", json=payload)
        resp.raise_for_status()
        return resp.json()

    def list_subscribers(self) -> Dict[str, Any]:
        """List all registered artisanal fishermen and current voyage statuses."""
        resp = self._client.get("/v1/telecom/subscribers")
        resp.raise_for_status()
        return resp.json()

    def ingest_carrier_webhook(
        self,
        payload: Dict[str, Any],
        secret: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send authenticated cell tower handover / location update from carrier."""
        raw_body = json.dumps(payload).encode("utf-8")
        headers = {}
        if secret:
            sig = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
            headers["X-Carrier-Signature"] = sig

        resp = self._client.post("/v1/telecom/carrier-webhook", content=raw_body, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def ingest_ais_nmea(self, raw_nmea: str) -> Dict[str, Any]:
        """Ingest raw NMEA 0183 !AIVDM radio sentence from coastal VHF receiver."""
        resp = self._client.post("/v1/ais/nmea", json={"raw_nmea": raw_nmea})
        resp.raise_for_status()
        return resp.json()

    def declare_sar_incident(
        self,
        phone_number: str,
        reason: str = "OVERDUE_VOYAGE_MOTOR_FAILURE",
        hours_adrift: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Declare emergency Search and Rescue operation with Guinea Current drift calculation."""
        payload = {
            "phone_number": phone_number,
            "reason": reason,
            "hours_adrift": hours_adrift,
        }
        resp = self._client.post("/v1/sar/declare", json=payload)
        resp.raise_for_status()
        return resp.json()

    def list_sar_incidents(self, active_only: bool = True) -> Dict[str, Any]:
        """List active or historical Search and Rescue incidents."""
        resp = self._client.get("/v1/sar/incidents", params={"active_only": active_only})
        resp.raise_for_status()
        return resp.json()

    def resolve_sar_incident(self, incident_id: str) -> Dict[str, Any]:
        """Resolve a Search and Rescue incident."""
        resp = self._client.post(f"/v1/sar/resolve/{incident_id}")
        resp.raise_for_status()
        return resp.json()

    def dispatch_voice_alert(self, phone_number: str, cpa_nm: float, tcpa_min: float) -> Dict[str, Any]:
        """Dispatch mother-tongue automated robocall alert."""
        payload = {
            "phone_number": phone_number,
            "cpa_nm": cpa_nm,
            "tcpa_min": tcpa_min,
        }
        resp = self._client.post("/v1/telecom/alert/voice", json=payload)
        resp.raise_for_status()
        return resp.json()

    def clear_fleet(self) -> Dict[str, Any]:
        """Clear active vessels and radar markers."""
        resp = self._client.post("/v1/fleet/clear")
        resp.raise_for_status()
        return resp.json()
