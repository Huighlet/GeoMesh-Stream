"""
GeoMesh-Stream: Standalone Fleet Telemetry & Spatial Geofence Server.
Includes REST API endpoints, NMEA 0183 AIS radio ingestion, Search and Rescue (SAR)
Guinea Current drift extrapolation, and an embedded interactive Leaflet.js web dashboard.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field

from geomesh.ais import AISDecoder, AISPositionReport, AISStaticReport
from geomesh.engine import GeoMeshEngine
from geomesh.geofence import PolygonGeofence, RadialGeofence
from geomesh.sar import GuineaCurrentSARModel, SARWatchdog
from geomesh.storage import DurableStore, SARIncidentRecord, SubscriberRecord
from geomesh.telecom import (
    Language,
    MaritimeVoiceAlertService,
    TelecomPassiveGateway,
    VesselStatus,
)
from geomesh.types import (
    GeofenceAlreadyExistsError,
    GeofenceNotFoundError,
    InvalidPolygonError,
    Point,
    TelemetryPing,
)

app = FastAPI(
    title="GeoMesh-Stream Ghana Maritime Defense & Safety Engine",
    description="Real-Time Ghana Maritime Vessel Stream, Zero-Touch Telecom LBS & SAR Engine.",
    version="1.0.0",
)

# Global engine instance, durable storage, and gateways
_engine = GeoMeshEngine(cell_size_deg=0.02)
durable_store = DurableStore()
telecom_gateway = TelecomPassiveGateway(store=durable_store)
sar_watchdog = SARWatchdog(durable_store)
ais_decoder = AISDecoder()

_telemetry_pings_total = 0
_geofence_breaches_total = 0

# Clean state backed by SQLite WAL database - no hardcoded mock fishermen


class RateLimiter:
    """Thread-safe Token Bucket Rate Limiter."""

    def __init__(self, rate: float = 5000.0, capacity: float = 5000.0):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> Tuple[bool, int]:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True, int(self.tokens)
            return False, 0


rate_limiter = RateLimiter(rate=5000.0, capacity=5000.0)


@app.middleware("http")
async def enterprise_security_middleware(request: Request, call_next):
    """Enforces rate limiting and API key verification."""
    if request.url.path in ["/health", "/docs", "/openapi.json", "/", "/metrics"]:
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = "5000"
        response.headers["X-RateLimit-Remaining"] = "5000"
        return response

    allowed, remaining = rate_limiter.acquire()
    if not allowed:
        return Response(
            content='{"detail": "Rate limit exceeded"}',
            status_code=429,
            media_type="application/json",
            headers={"Retry-After": "1"},
        )

    configured_key = os.getenv("GEOMESH_API_KEY")
    if configured_key and request.url.path.startswith("/v1/"):
        req_key = request.headers.get("X-API-Key")
        if req_key != configured_key:
            return Response(
                content='{"detail": "Unauthorized: Invalid or missing X-API-Key header"}',
                status_code=401,
                media_type="application/json",
            )

    response = await call_next(request)
    response.headers["X-RateLimit-Limit"] = "5000"
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    return response


@app.get("/metrics", response_class=PlainTextResponse, tags=["Observability"])
def get_metrics() -> PlainTextResponse:
    """Prometheus metrics endpoint for geospatial ingestion throughput and geofence breaches."""
    engine = get_engine()
    geofences_count = engine.geofences.count_geofences()
    entities_count = len(engine._entity_positions)
    cells_count = len(engine.entity_index._grid)

    lines = [
        "# HELP geomesh_geofences_total Total registered geofences",
        "# TYPE geomesh_geofences_total gauge",
        f"geomesh_geofences_total {geofences_count}",
        "# HELP geomesh_tracked_entities_total Total moving entities tracked in spatial mesh",
        "# TYPE geomesh_tracked_entities_total gauge",
        f"geomesh_tracked_entities_total {entities_count}",
        "# HELP geomesh_spatial_cells_total Total active spatial hash grid cells",
        "# TYPE geomesh_spatial_cells_total gauge",
        f"geomesh_spatial_cells_total {cells_count}",
        "# HELP geomesh_telemetry_pings_total Total ingested GPS telemetry pings",
        "# TYPE geomesh_telemetry_pings_total counter",
        f"geomesh_telemetry_pings_total {_telemetry_pings_total}",
        "# HELP geomesh_geofence_breaches_total Total geofence alerts triggered",
        "# TYPE geomesh_geofence_breaches_total counter",
        f"geomesh_geofence_breaches_total {_geofence_breaches_total}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n")


def get_engine() -> GeoMeshEngine:
    return _engine


# -------------------------------------------------------------------------
# Request / Response Schemas
# -------------------------------------------------------------------------

class RadialGeofenceRequest(BaseModel):
    geofence_id: str = Field(..., description="Unique geofence ID")
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    radius_m: float = Field(..., gt=0.0, description="Radius in meters")
    name: str = Field(default="", description="Human-readable name")


class PointSchema(BaseModel):
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)


class PolygonGeofenceRequest(BaseModel):
    geofence_id: str = Field(..., description="Unique geofence ID")
    vertices: List[PointSchema] = Field(..., min_length=3, description="Ordered boundary vertices")
    name: str = Field(default="", description="Human-readable name")


class TelemetryPingRequest(BaseModel):
    entity_id: str = Field(..., description="Unique vehicle/asset ID")
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    speed_mps: Optional[float] = Field(default=None, ge=0.0, description="Speed in meters/second")
    heading_deg: Optional[float] = Field(default=None, ge=0.0, le=360.0, description="Heading in degrees (0..360)")
    timestamp_ns: Optional[int] = Field(default=None, description="Nanosecond timestamp (defaults to now)")


class RegisterSubscriberRequest(BaseModel):
    phone_number: str = Field(..., description="Ghanaian mobile number (e.g. 0241234567)")
    canoe_name: str = Field(..., description="Canoe name / registration (e.g. Nyimdzii)")
    landing_beach: str = Field(default="Keta Beach", description="Landing beach")
    language: str = Field(default="EWE", description="Primary mother tongue (EWE, GA, FANTE, ENGLISH)")


class CellEventRequest(BaseModel):
    phone_number: str = Field(..., description="Subscriber phone number")
    tower_id: str = Field(..., description="Cell tower sector ID")
    timing_advance: int = Field(default=0, ge=0, le=64, description="GSM Timing Advance (0..64, each step = 550m)")
    timestamp_ns: Optional[int] = None


class VoiceAlertRequest(BaseModel):
    phone_number: str
    cpa_nm: float
    tcpa_min: float


class AISNMEARequest(BaseModel):
    raw_nmea: str = Field(..., description="Single or multi-line NMEA 0183 !AIVDM string")


class SARDeclareRequest(BaseModel):
    phone_number: str = Field(..., description="Registered fisherman's phone number")
    reason: str = Field(default="OVERDUE_VOYAGE_MOTOR_FAILURE", description="SAR incident declaration reason")
    hours_adrift: Optional[float] = Field(default=None, description="Estimated hours adrift")


class WeatherBroadcastRequest(BaseModel):
    landing_beach: str = Field(default="Keta Beach")
    hazard: str = Field(default="SWELL_WARNING")
    wave_height_m: float = Field(default=2.5)
    wind_speed_kt: float = Field(default=22.0)


# -------------------------------------------------------------------------
# REST API Endpoints
# -------------------------------------------------------------------------

@app.get("/health", tags=["Health"])
def health_check() -> Dict[str, Any]:
    engine = get_engine()
    return {
        "status": "HEALTHY",
        "engine": "GeoMesh-Stream",
        "version": "1.0.0",
        "active_geofences": engine.geofences.count_geofences(),
        "indexed_entities": engine.entity_index.total_items,
        "durable_store": "SQLite WAL Active",
    }


@app.post("/v1/geofences/radial", status_code=status.HTTP_201_CREATED, tags=["Geofences"])
def create_radial_geofence(req: RadialGeofenceRequest) -> Dict[str, Any]:
    engine = get_engine()
    fence = RadialGeofence(
        geofence_id=req.geofence_id,
        center=Point(req.lat, req.lon),
        radius_m=req.radius_m,
        name=req.name,
    )
    try:
        engine.register_geofence(fence)
        return {
            "geofence_id": fence.geofence_id,
            "name": fence.name,
            "type": "RADIAL",
            "center": {"lat": fence.center.lat, "lon": fence.center.lon},
            "radius_m": fence.radius_m,
        }
    except GeofenceAlreadyExistsError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))


@app.post("/v1/geofences/polygon", status_code=status.HTTP_201_CREATED, tags=["Geofences"])
def create_polygon_geofence(req: PolygonGeofenceRequest) -> Dict[str, Any]:
    engine = get_engine()
    verts = [Point(v.lat, v.lon) for v in req.vertices]
    try:
        fence = PolygonGeofence(geofence_id=req.geofence_id, vertices=verts, name=req.name)
        engine.register_geofence(fence)
        return {
            "geofence_id": fence.geofence_id,
            "name": fence.name,
            "type": "POLYGON",
            "vertex_count": len(fence.vertices),
        }
    except GeofenceAlreadyExistsError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    except InvalidPolygonError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@app.post("/v1/telemetry", tags=["Telemetry"])
def ingest_telemetry(req: TelemetryPingRequest) -> Dict[str, Any]:
    global _telemetry_pings_total, _geofence_breaches_total
    _telemetry_pings_total += 1

    engine = get_engine()
    ts = req.timestamp_ns if req.timestamp_ns is not None else time.time_ns()
    ping = TelemetryPing.create(
        entity_id=req.entity_id,
        lat=req.lat,
        lon=req.lon,
        speed_mps=req.speed_mps,
        heading_deg=req.heading_deg,
        timestamp_ns=ts,
    )

    alerts = engine.ingest_telemetry(ping)
    if alerts:
        _geofence_breaches_total += len(alerts)
    state = engine.get_kinematic_state(req.entity_id)

    return {
        "entity_id": req.entity_id,
        "current_position": {"lat": req.lat, "lon": req.lon},
        "kinematics": {
            "velocity_mps": state.velocity_mps if state else 0.0,
            "bearing_deg": state.bearing_deg if state else 0.0,
            "acceleration_mps2": state.acceleration_mps2 if state else 0.0,
        },
        "alerts": [
            {
                "alert_id": a.alert_id,
                "geofence_id": a.geofence_id,
                "event": str(a.event),
                "dwell_duration_sec": a.dwell_duration_sec,
            }
            for a in alerts
        ],
    }


@app.get("/v1/geofences/export", tags=["Geofences"])
def export_geofences() -> Dict[str, Any]:
    """Export all active geofence geometries for auditing and disaster recovery."""
    engine = get_engine()
    fences_data = []
    for fence_id, fence in engine.geofences._geofences.items():
        f_info = {
            "geofence_id": fence.geofence_id,
            "name": fence.name,
            "type": str(fence.fence_type),
        }
        if isinstance(fence, RadialGeofence):
            f_info["center"] = {"lat": fence.center.lat, "lon": fence.center.lon}
            f_info["radius_m"] = fence.radius_m
        elif isinstance(fence, PolygonGeofence):
            f_info["vertices"] = [{"lat": v.lat, "lon": v.lon} for v in fence.vertices]
        fences_data.append(f_info)

    return {
        "timestamp_ns": time.time_ns(),
        "total_geofences": len(fences_data),
        "geofences": fences_data,
    }


@app.api_route("/v1/fleet/snapshot", methods=["GET", "POST"], tags=["Spatial Queries"])
def snapshot_fleet(request: Request) -> Dict[str, Any]:
    """Capture or view point-in-time state checkpoint of all tracked fleet vessels, positions, and kinematics."""
    engine = get_engine()
    snapshot_id = f"snap_fleet_{int(time.time_ns())}"
    vehicles = []

    for ent_id, pos in engine._entity_positions.items():
        k_state = engine.get_kinematic_state(ent_id)
        pred_30s = engine.predict_position(ent_id, 30.0)
        active_fences = list(engine.geofences._active_presence.get(ent_id, {}))

        vehicles.append({
            "entity_id": ent_id,
            "position": {"lat": pos.lat, "lon": pos.lon},
            "kinematics": {
                "velocity_mps": k_state.velocity_mps if k_state else 0.0,
                "bearing_deg": k_state.bearing_deg if k_state else 0.0,
                "acceleration_mps2": k_state.acceleration_mps2 if k_state else 0.0,
            },
            "predicted_position_30s": {"lat": pred_30s.lat, "lon": pred_30s.lon} if pred_30s else None,
            "active_geofences": active_fences,
        })

    return {
        "snapshot_id": snapshot_id,
        "timestamp_ns": time.time_ns(),
        "total_tracked_vehicles": len(vehicles),
        "vehicles": vehicles,
        "status": "FLEET_SNAPSHOT_COMMITTED",
    }


@app.post("/v1/fleet/clear", tags=["Spatial Queries"])
def clear_fleet_test_entities() -> Dict[str, Any]:
    """Reset tracked entities and clear active vessels from spatial index."""
    engine = get_engine()
    count = len(engine._entity_positions)
    for ent_id in list(engine._entity_positions.keys()):
        engine.remove_entity(ent_id)
    return {"cleared": True, "entities_removed": count}


@app.get("/v1/proximity", tags=["Spatial Queries"])
def query_proximity(
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
    radius_m: float = Query(..., gt=0.0),
) -> Dict[str, Any]:
    engine = get_engine()
    results = engine.query_radius(Point(lat, lon), radius_m=radius_m)
    return {
        "center": {"lat": lat, "lon": lon},
        "radius_m": radius_m,
        "match_count": len(results),
        "entities": [
            {"entity_id": ent_id, "lat": pt.lat, "lon": pt.lon, "distance_meters": round(dist, 1)}
            for ent_id, pt, dist in results
        ],
    }


@app.get("/v1/predict/{entity_id}", tags=["Kinematics"])
def predict_position(
    entity_id: str,
    future_delta_sec: float = Query(30.0, gt=0.0),
) -> Dict[str, Any]:
    engine = get_engine()
    predicted = engine.predict_position(entity_id, future_delta_sec)
    if not predicted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No kinematic telemetry found for entity '{entity_id}'.",
        )
    return {
        "entity_id": entity_id,
        "future_delta_sec": future_delta_sec,
        "predicted_position": {"lat": predicted.lat, "lon": predicted.lon},
    }


@app.get("/v1/maritime/cpa", tags=["Maritime Safety"])
def get_collision_risk(
    vessel_1: str = Query(..., description="First vessel ID / Tanker MMSI"),
    vessel_2: str = Query(..., description="Second vessel ID / Artisanal Canoe ID"),
) -> Dict[str, Any]:
    """Calculate Closest Point of Approach (CPA) and Time to CPA (TCPA) between two vessels."""
    engine = get_engine()
    res = engine.calculate_cpa_tcpa(vessel_1, vessel_2)
    if not res:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Kinematic telemetry missing for '{vessel_1}' or '{vessel_2}'.",
        )

    cpa_m, tcpa_s = res
    cpa_nm = round(cpa_m / 1852.0, 2)
    tcpa_min = round(tcpa_s / 60.0, 1)

    risk_level = "LOW"
    if cpa_nm < 1.0 and tcpa_min < 15.0:
        risk_level = "CRITICAL_COLLISION_ALERT"
    elif cpa_nm < 2.5 and tcpa_min < 30.0:
        risk_level = "WARNING_CONVERGING"

    return {
        "vessel_1": vessel_1,
        "vessel_2": vessel_2,
        "cpa_meters": round(cpa_m, 1),
        "cpa_nautical_miles": cpa_nm,
        "tcpa_seconds": round(tcpa_s, 1),
        "tcpa_minutes": tcpa_min,
        "risk_level": risk_level,
    }


# -------------------------------------------------------------------------
# Telecom Passive LBS & Artisanal Fishermen Registry
# -------------------------------------------------------------------------

@app.post("/v1/telecom/register", status_code=status.HTTP_201_CREATED, tags=["Telecom LBS & Artisanal Registry"])
def register_subscriber(req: RegisterSubscriberRequest) -> Dict[str, Any]:
    """
    One-Time Lifetime Enrollment for Artisanal Fishermen.
    Fishermen register phone number once. The system tracks departures automatically via passive cell handovers.
    """
    lang_str = req.language.upper()
    lang = Language(lang_str) if lang_str in Language._value2member_map_ else Language.EWE
    sub = telecom_gateway.register_subscriber(
        phone_number=req.phone_number,
        canoe_name=req.canoe_name,
        landing_beach=req.landing_beach,
        language=lang,
    )
    return {
        "registered": True,
        "phone_number": sub.phone_number,
        "canoe_name": sub.canoe_name,
        "landing_beach": sub.landing_beach,
        "language": str(sub.language),
        "status": str(sub.status),
        "policy": "ONE_TIME_ENROLLMENT: Zero daily dials required from fisherman.",
    }


@app.get("/v1/telecom/subscribers", tags=["Telecom LBS & Artisanal Registry"])
def list_subscribers() -> Dict[str, Any]:
    """Retrieve all enrolled Ghanaian artisanal fishermen and their real-time sea/shore status."""
    subs = telecom_gateway.list_subscribers()
    return {
        "total_subscribers": len(subs),
        "subscribers": [
            {
                "phone_number": s.phone_number,
                "canoe_name": s.canoe_name,
                "landing_beach": s.landing_beach,
                "language": str(s.language),
                "status": str(s.status),
                "last_seen_tower": s.last_seen_tower,
                "last_timing_advance": s.last_timing_advance,
                "last_point": {"lat": s.last_point.lat, "lon": s.last_point.lon} if s.last_point else None,
                "distance_offshore_km": round((s.last_timing_advance * 550.0) / 1000.0, 2),
            }
            for s in subs
        ],
    }


@app.post("/v1/telecom/cell-event", tags=["Telecom LBS & Artisanal Registry"])
def ingest_cell_event(req: CellEventRequest) -> Dict[str, Any]:
    """
    Process passive GSM cell tower event (Handover, Location Update, or Silent Paging).
    Automatically derives coordinate from Timing Advance and updates spatial tracking mesh.
    """
    res = telecom_gateway.process_passive_cell_event(
        phone_number=req.phone_number,
        tower_id=req.tower_id,
        timing_advance=req.timing_advance,
        timestamp_ns=req.timestamp_ns,
    )

    # Ingest derived coordinate into spatial engine
    pos = res["derived_position"]
    ping = TelemetryPing.create(
        entity_id=f"canoe-{res['phone_number']}",
        lat=pos["lat"],
        lon=pos["lon"],
        speed_mps=1.8 if res["status"] == "AT_SEA" else 0.0,
        heading_deg=165.0,
        timestamp_ns=res["timestamp_ns"],
    )
    engine = get_engine()
    alerts = engine.ingest_telemetry(ping)
    res["spatial_alerts"] = [{"event": str(a.event), "geofence_id": a.geofence_id} for a in alerts]
    return res


@app.post("/v1/telecom/carrier-webhook", tags=["Telecom LBS & Artisanal Registry"])
async def receive_carrier_webhook(request: Request) -> Dict[str, Any]:
    """Authenticated incoming webhook from Ghana telco aggregators (MTN/Telecel/Hubtel)."""
    raw_body = await request.body()
    sig = request.headers.get("X-Carrier-Signature") or request.headers.get("X-Hubtel-Signature")
    secret = os.getenv("CARRIER_WEBHOOK_SECRET")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload in carrier webhook.")

    try:
        res = telecom_gateway.process_carrier_webhook(
            payload=payload,
            raw_body=raw_body,
            signature_header=sig,
            secret=secret,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pos = res["derived_position"]
    ping = TelemetryPing.create(
        entity_id=f"canoe-{res['phone_number']}",
        lat=pos["lat"],
        lon=pos["lon"],
        speed_mps=1.8 if res["status"] == "AT_SEA" else 0.0,
        heading_deg=165.0,
        timestamp_ns=res["timestamp_ns"],
    )
    engine = get_engine()
    alerts = engine.ingest_telemetry(ping)
    res["spatial_alerts"] = [{"event": str(a.event), "geofence_id": a.geofence_id} for a in alerts]
    return res


@app.post("/v1/telecom/alert/voice", tags=["Telecom LBS & Artisanal Registry"])
def dispatch_voice_alert(req: VoiceAlertRequest) -> Dict[str, Any]:
    """Dispatch automated multilingual emergency voice call (IVR robocall) in Ewe, Ga, Fante, or English."""
    sub = telecom_gateway.get_subscriber(req.phone_number)
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Subscriber with phone '{req.phone_number}' not found in registry.",
        )

    res = telecom_gateway._voice_service.dispatch_alert_call(
        subscriber=sub,
        threat_vessel="Commercial Vessel (AIS Traffic)",
        cpa_nm=req.cpa_nm,
        tcpa_min=req.tcpa_min,
    )
    return res


# -------------------------------------------------------------------------
# AIS Radio Receiver Ingestion
# -------------------------------------------------------------------------

@app.post("/v1/ais/nmea", tags=["AIS Commercial Ship Telemetry"])
def ingest_ais_nmea(req: AISNMEARequest) -> Dict[str, Any]:
    """Decode raw NMEA 0183 !AIVDM VHF radio burst and stream into spatial mesh."""
    report = ais_decoder.decode_sentence(req.raw_nmea)
    if not report:
        raise HTTPException(
            status_code=400,
            detail="Failed to decode NMEA 0183 AIVDM sentence (invalid checksum, length, or unsupported type).",
        )

    if isinstance(report, AISPositionReport):
        engine = get_engine()
        ping = report.to_telemetry_ping()
        alerts = engine.ingest_telemetry(ping)
        return {
            "decoded": True,
            "type": "POSITION_REPORT",
            "mmsi": report.mmsi,
            "entity_id": ping.entity_id,
            "lat": report.lat,
            "lon": report.lon,
            "speed_knots": report.sog_knots,
            "heading_deg": report.heading_deg,
            "nav_status": report.nav_status,
            "alerts": [{"event": str(a.event), "geofence_id": a.geofence_id} for a in alerts],
        }
    elif isinstance(report, AISStaticReport):
        return {
            "decoded": True,
            "type": "STATIC_VOYAGE_DATA",
            "mmsi": report.mmsi,
            "imo": report.imo,
            "vessel_name": report.vessel_name,
            "call_sign": report.call_sign,
            "ship_type": report.ship_type,
            "destination": report.destination,
        }
    return {"decoded": False}


# -------------------------------------------------------------------------
# Search and Rescue (SAR) & Weather Warnings
# -------------------------------------------------------------------------

@app.post("/v1/sar/declare", status_code=status.HTTP_201_CREATED, tags=["Search and Rescue (SAR)"])
def declare_sar_incident(req: SARDeclareRequest) -> Dict[str, Any]:
    """Declare Search and Rescue incident, extrapolating Guinea Current drift datum for Ghana Navy/GMA."""
    try:
        rec = sar_watchdog.declare_incident(
            phone_number=req.phone_number,
            reason=req.reason,
            custom_hours_adrift=req.hours_adrift,
        )
        return {
            "incident_id": rec.incident_id,
            "phone_number": rec.phone_number,
            "canoe_name": rec.canoe_name,
            "landing_beach": rec.landing_beach,
            "status": rec.status,
            "last_known_fix": {"lat": rec.last_known_lat, "lon": rec.last_known_lon},
            "projected_datum": {"lat": rec.datum_lat, "lon": rec.datum_lon},
            "drift_distance_nm": rec.drift_distance_nm,
            "search_radius_nm": rec.search_radius_nm,
            "docket": json.loads(rec.gma_docket) if rec.gma_docket else {},
        }
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/v1/sar/incidents", tags=["Search and Rescue (SAR)"])
def list_sar_incidents(active_only: bool = Query(True)) -> Dict[str, Any]:
    """List Search and Rescue incidents with projected drift coordinates."""
    incidents = durable_store.list_sar_incidents(active_only=active_only)
    return {
        "total_incidents": len(incidents),
        "incidents": [
            {
                "incident_id": inc.incident_id,
                "phone_number": inc.phone_number,
                "canoe_name": inc.canoe_name,
                "landing_beach": inc.landing_beach,
                "declared_at": inc.declared_at,
                "last_known_fix": {"lat": inc.last_known_lat, "lon": inc.last_known_lon},
                "projected_datum": {"lat": inc.datum_lat, "lon": inc.datum_lon},
                "drift_distance_nm": inc.drift_distance_nm,
                "search_radius_nm": inc.search_radius_nm,
                "status": inc.status,
                "docket": json.loads(inc.gma_docket) if inc.gma_docket else {},
            }
            for inc in incidents
        ],
    }


@app.post("/v1/sar/resolve/{incident_id}", tags=["Search and Rescue (SAR)"])
def resolve_sar_incident(incident_id: str) -> Dict[str, Any]:
    """Close an active SAR incident upon safe recovery."""
    success = durable_store.resolve_sar_incident(incident_id, "RESOLVED")
    if not success:
        raise HTTPException(status_code=404, detail=f"SAR incident '{incident_id}' not found.")
    return {"resolved": True, "incident_id": incident_id, "status": "RESOLVED"}


@app.post("/v1/weather/broadcast", tags=["Maritime Safety"])
def broadcast_weather_alert(req: WeatherBroadcastRequest) -> Dict[str, Any]:
    """Broadcast high-priority marine safety alert to all canoes currently at sea from a landing beach."""
    subs = telecom_gateway.list_subscribers()
    notified = []
    for s in subs:
        if s.status == VesselStatus.AT_SEA and (req.landing_beach.lower() in s.landing_beach.lower() or req.landing_beach == "ALL"):
            notified.append({
                "phone_number": s.phone_number,
                "canoe_name": s.canoe_name,
                "language": str(s.language),
                "alert": f"DANGER: {req.hazard} - Wave swell {req.wave_height_m}m, winds {req.wind_speed_kt}kt. Head for shore immediately.",
            })
    return {
        "hazard": req.hazard,
        "landing_beach": req.landing_beach,
        "total_canoes_notified": len(notified),
        "dispatches": notified,
    }


# -------------------------------------------------------------------------
# Embedded Interactive Leaflet.js Production Map Dashboard
# -------------------------------------------------------------------------

class SOSRequest(BaseModel):
    phone_number: str = Field(..., description="Subscriber phone number triggering SOS")
    message: str = Field(default="SOS_MAYDAY_ENGINE_FAILURE", description="Distress message or reason")


@app.post("/v1/telecom/sos", tags=["Telecom LBS & Artisanal Registry"])
def trigger_keypad_sos(req: SOSRequest) -> Dict[str, Any]:
    """Process incoming emergency SOS distress signal sent from a keypad phone (SMS or USSD *920*44#)."""
    res = telecom_gateway.trigger_sos(phone_number=req.phone_number, message=req.message)
    # Immediately declare SAR distress incident docket for Ghana Navy & GMA
    sar_inc = sar_watchdog.declare_incident(
        phone_number=req.phone_number,
        reason=f"EMERGENCY_KEYPAD_SOS: {req.message}",
        custom_hours_adrift=0.5,
    )
    res["sar_incident_id"] = sar_inc.incident_id
    res["projected_datum"] = {"lat": sar_inc.datum_lat, "lon": sar_inc.datum_lon}
    res["search_radius_nm"] = sar_inc.search_radius_nm
    return res


@app.delete("/v1/telecom/subscribers/{phone_number}", tags=["Telecom LBS & Artisanal Registry"])
def delete_subscriber(phone_number: str) -> Dict[str, Any]:
    """Unenroll / delete a registered artisanal canoe from system memory and database."""
    success = telecom_gateway.delete_subscriber(phone_number)
    return {"deleted": success, "phone_number": phone_number}


@app.get("/v1/telecom/towers", tags=["Telecom LBS & Artisanal Registry"])
def list_coastal_towers() -> Dict[str, Any]:
    """Retrieve all coastal GSM/LTE cell towers spanning the 550km Ghana coastline."""
    towers = telecom_gateway.list_towers()
    return {
        "total_towers": len(towers),
        "towers": [
            {
                "tower_id": t.tower_id,
                "name": t.name,
                "lat": t.lat,
                "lon": t.lon,
                "sector_type": t.sector_type,
                "azimuth_deg": t.azimuth_deg,
                "region": t.region,
            }
            for t in towers
        ],
    }
# -------------------------------------------------------------------------
# Embedded Interactive Leaflet.js Production Map Dashboard
# -------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
def get_map_dashboard() -> str:
    """Serve GH-MARSAR National Coastal Operations Command Center."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>GH-MARSAR: Ghana National Maritime Rescue & Artisanal Safety Network (GeoMesh-Stream)</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    body { margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #060a12; color: #f8fafc; }
    #container { display: flex; height: 100vh; flex-direction: column; }
    
    /* Top National Header */
    #topbar { background: #0b1120; border-bottom: 2px solid #1e293b; padding: 10px 20px; display: flex; justify-content: space-between; align-items: center; }
    .gh-flag { display: inline-flex; height: 14px; width: 22px; border-radius: 2px; overflow: hidden; margin-right: 8px; vertical-align: middle; }
    .gh-red { flex: 1; background: #ce1126; }
    .gh-yellow { flex: 1; background: #fcd116; }
    .gh-green { flex: 1; background: #006b3f; }
    .stat-badge { background: #1e293b; border: 1px solid #334155; padding: 6px 12px; border-radius: 6px; font-size: 0.76rem; display: inline-flex; align-items: center; gap: 6px; margin-left: 8px; }
    .stat-val { font-weight: 700; color: #38bdf8; font-size: 0.85rem; }
    .stat-sos { color: #ef4444; font-weight: 800; }

    /* Sector Navigation Bar */
    #navsectors { background: #0f172a; border-bottom: 1px solid #1e293b; padding: 6px 20px; display: flex; gap: 8px; font-size: 0.74rem; overflow-x: auto; }
    .nav-btn { background: #1e293b; color: #94a3b8; border: 1px solid #334155; padding: 4px 10px; border-radius: 4px; cursor: pointer; white-space: nowrap; font-weight: 600; }
    .nav-btn:hover, .nav-btn.active { background: #0284c7; color: #fff; border-color: #38bdf8; }

    #main { display: flex; flex: 1; height: calc(100vh - 90px); }
    #sidebar { width: 440px; background: #0b1120; padding: 16px; overflow-y: auto; border-right: 1px solid #1e293b; box-shadow: 2px 0 10px rgba(0,0,0,0.5); }
    #map { flex-grow: 1; height: 100%; }

    .card { background: #131c2e; border: 1px solid #1e293b; border-radius: 6px; padding: 12px; margin-bottom: 12px; }
    .card-emergency { border-color: #ef4444; background: #1c131a; box-shadow: 0 0 10px rgba(239,68,68,0.15); }
    .card h3 { margin-top: 0; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.05em; color: #94a3b8; margin-bottom: 8px; display:flex; justify-content:space-between; align-items:center; }
    .card-emergency h3 { color: #f87171; }
    
    .btn { background: #0284c7; color: white; border: none; padding: 8px 12px; border-radius: 5px; cursor: pointer; width: 100%; font-weight: 600; font-size: 0.8rem; margin-bottom: 8px; transition: background 0.2s; }
    .btn:hover { background: #0369a1; }
    .btn-danger { background: #dc2626; }
    .btn-danger:hover { background: #b91c1c; }
    .btn-sm { padding: 3px 6px; font-size: 0.68rem; width: auto; margin-bottom: 0; }
    
    .event-log { font-family: monospace; font-size: 0.73rem; max-height: 180px; overflow-y: auto; background: #030712; padding: 8px; border-radius: 4px; line-height: 1.5; border: 1px solid #1e293b; }
    .event-cpa { color: #ef4444; font-weight: 700; }
    .event-sar { color: #f59e0b; font-weight: 700; }
    .event-info { color: #38bdf8; }
    .event-enter { color: #10b981; }

    /* Native dark tiles */
    .leaflet-container { background: #070d19; }
    .zone-pill { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 0.66rem; margin-right: 4px; margin-bottom: 4px; }
  </style>
</head>
<body>
  <div id="container">
    <!-- Top National Command Bar -->
    <div id="topbar">
      <div style="display:flex;align-items:center;">
        <span class="gh-flag"><span class="gh-red"></span><span class="gh-yellow"></span><span class="gh-green"></span></span>
        <div>
          <strong style="color:#f8fafc;font-size:0.95rem;letter-spacing:0.02em;">GH-MARSAR &bull; GHANA NATIONAL MARITIME RESCUE &amp; COASTAL SAFETY NETWORK</strong>
          <div style="font-size:0.68rem;color:#94a3b8;">550km Gulf of Guinea Coastline &bull; Fisheries Act 625 6NM Exclusion Zone &bull; Keypad Feature Phone LBS</div>
        </div>
      </div>
      <div>
        <div class="stat-badge">ENROLLED FLEET: <span id="statEnrolled" class="stat-val">0</span></div>
        <div class="stat-badge">AT SEA: <span id="statAtSea" class="stat-val" style="color:#38bdf8;">0</span></div>
        <div class="stat-badge">ON SHORE: <span id="statOnShore" class="stat-val" style="color:#10b981;">0</span></div>
        <div class="stat-badge" style="border-color:#ef4444;">🚨 OVERDUE / SOS: <span id="statSOS" class="stat-sos">0</span></div>
      </div>
    </div>

    <!-- Coastal Sector Quick Navigation -->
    <div id="navsectors">
      <span style="color:#64748b;line-height:24px;font-weight:600;">SECTOR JUMP:</span>
      <button class="nav-btn active" onclick="zoomSector('ALL', this)">🇬🇭 Entire Ghana Coast (550km)</button>
      <button class="nav-btn" onclick="zoomSector('WESTERN', this)">Western Region (Half Assini &bull; Axim &bull; Takoradi)</button>
      <button class="nav-btn" onclick="zoomSector('CENTRAL', this)">Central Region (Elmina &bull; Cape Coast &bull; Winneba)</button>
      <button class="nav-btn" onclick="zoomSector('ACCRA', this)">Greater Accra (Jamestown &bull; Tema &bull; Ada)</button>
      <button class="nav-btn" onclick="zoomSector('VOLTA', this)">Volta Region (Cape St. Paul &bull; Keta &bull; Aflao)</button>
    </div>

    <div id="main">
      <div id="sidebar">

        <!-- 1. EMERGENCY MISSING BOAT & SAR HOTLINE -->
        <div class="card card-emergency">
          <h3>🚨 MISSING BOAT AT SEA / REPORT OVERDUE CANOE <span style="font-size:0.65rem;background:#dc2626;color:#fff;padding:2px 6px;border-radius:4px;">GMA &amp; NAVY</span></h3>
          <p style="font-size:0.72rem;color:#cbd5e1;line-height:1.4;margin:4px 0 8px 0;">
            If an artisanal canoe fails to return from night fishing or engine breaks down, enter boat details below. The system pulls the last tower contact, extrapolates the <strong>Guinea Current drift datum</strong>, and alerts the Ghana Navy.
          </p>
          <div style="display:flex;gap:6px;margin-bottom:8px;">
            <input id="sarInput" type="text" placeholder="Phone or Canoe Name (e.g. Nyame Nti)" style="flex:1;background:#090d16;border:1px solid #475569;color:#fff;padding:6px;border-radius:4px;font-size:0.75rem;" />
            <button class="btn btn-danger btn-sm" onclick="reportMissingBoat()" style="white-space:nowrap;">DECLARE SAR</button>
          </div>
          <div id="activeSARBox" style="font-size:0.72rem;max-height:110px;overflow-y:auto;line-height:1.4;color:#94a3b8;border-top:1px solid #334155;padding-top:6px;">
            No active missing boat incidents.
          </div>
        </div>

        <!-- 2. ONE-TIME CANOE REGISTRATION -->
        <div class="card">
          <h3>Enroll Artisanal Canoe (Feature Phone) <span style="font-size:0.65rem;background:#059669;color:#fff;padding:2px 6px;border-radius:4px;">ZERO DAILY DIAL</span></h3>
          <div style="display:flex;flex-direction:column;gap:6px;font-size:0.75rem;">
            <input id="regPhone" type="text" placeholder="Phone Number (e.g. 0244123456)" style="background:#090d16;border:1px solid #334155;color:#fff;padding:6px;border-radius:4px;font-size:0.75rem;" />
            <input id="regCanoe" type="text" placeholder="Canoe Name (e.g. Nyame Nti)" style="background:#090d16;border:1px solid #334155;color:#fff;padding:6px;border-radius:4px;font-size:0.75rem;" />
            <div style="display:flex;gap:6px;">
              <select id="regBeach" style="flex:1;background:#090d16;border:1px solid #334155;color:#fff;padding:6px;border-radius:4px;font-size:0.74rem;">
                <option value="Keta Beach">Volta: Keta Beach</option>
                <option value="Dzelukope">Volta: Dzelukope</option>
                <option value="Anloga">Volta: Anloga</option>
                <option value="Aflao">Volta: Aflao</option>
                <option value="Ada Foah">Accra: Ada Foah</option>
                <option value="Prampram">Accra: Prampram</option>
                <option value="Tema Manhean">Accra: Tema Manhean</option>
                <option value="Jamestown">Accra: Jamestown</option>
                <option value="Chorkor">Accra: Chorkor</option>
                <option value="Winneba">Central: Winneba</option>
                <option value="Apam">Central: Apam</option>
                <option value="Saltpond">Central: Saltpond</option>
                <option value="Cape Coast">Central: Cape Coast</option>
                <option value="Elmina">Central: Elmina</option>
                <option value="Sekondi">Western: Sekondi</option>
                <option value="Takoradi">Western: Takoradi</option>
                <option value="Dixcove">Western: Dixcove</option>
                <option value="Axim">Western: Axim</option>
                <option value="Half Assini">Western: Half Assini</option>
              </select>
              <select id="regLang" style="flex:1;background:#090d16;border:1px solid #334155;color:#fff;padding:6px;border-radius:4px;font-size:0.74rem;">
                <option value="EWE">Ewe (Volta)</option>
                <option value="GA">Ga (Accra)</option>
                <option value="FANTE">Fante (Central)</option>
                <option value="DANGME">Dangme (Ada/Prampram)</option>
                <option value="NZEMA">Nzema (Western)</option>
                <option value="ENGLISH">English</option>
              </select>
            </div>
            <button onclick="enrollCanoe()" style="background:#4f46e5;color:#fff;border:none;padding:8px;border-radius:4px;cursor:pointer;font-weight:600;font-size:0.78rem;">Register Canoe Once (Permanent)</button>
          </div>
        </div>

        <!-- 3. HOW DOES IT PROTECT SMALL BOATS -->
        <div class="card" style="background:#0e1726;border-color:#1e293b;">
          <h3 style="color:#38bdf8;">How Are Artisanal Fishermen Protected?</h3>
          <ul style="font-size:0.72rem;color:#94a3b8;line-height:1.5;margin:0;padding-left:16px;">
            <li><strong style="color:#f8fafc;">Audible Night Voice Robocall:</strong> If a 100,000-ton ship enters 1.0 NM at night, the phone rings loudly speaking <em>Ewe, Ga, Fante, Dangme, or Nzema</em>.</li>
            <li><strong style="color:#f8fafc;">Zero-Touch Tripwire:</strong> Cell towers passively track departure and return when canoes switch between inland and seaward antennas. No dialing needed.</li>
            <li><strong style="color:#f8fafc;">Keypad SOS (*920*44#):</strong> In an emergency, sending SMS "SOS" to 1990 immediately triggers a Navy rescue alert.</li>
            <li><strong style="color:#f8fafc;">Guinea Current Drift Modeling:</strong> When engines fail, the system calculates the exact eastward ocean drift datum towards Togo.</li>
          </ul>
        </div>

        <!-- 4. ENROLLED CANOE ROSTER -->
        <div class="card">
          <h3>Enrolled Artisanal Roster <span id="rosterCount" style="color:#38bdf8;">0 boats</span></h3>
          <div id="fleetTable" style="font-size:0.72rem;max-height:160px;overflow-y:auto;line-height:1.4;">
            Loading registered canoes...
          </div>
        </div>

        <!-- 5. PRODUCTION FIELD TRIGGERS -->
        <div class="card">
          <h3>Production Operational Actions</h3>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;">
            <button class="btn btn-sm" onclick="simulateKeypadSOS()">Trigger Keypad SOS (*920*44#)</button>
            <button class="btn btn-sm" style="background:#0891b2;" onclick="injectAISVessel()">Inject AIS Tanker (Tema VHF)</button>
            <button class="btn btn-sm" style="background:#059669;" onclick="simulateSeawardTrip()">Simulate Seaward Launch</button>
            <button class="btn btn-sm" style="background:#334155;" onclick="clearRadar()">Clear Test Radar Marks</button>
          </div>
        </div>

        <!-- 6. AUDIT LOG -->
        <div class="card">
          <h3>Operational Audit Stream</h3>
          <div id="eventLog" class="event-log">National surveillance active across 550km coastline...</div>
        </div>

      </div>

      <div id="map"></div>
    </div>
  </div>

  <script>
    // National Map Initialization (Centered on Ghana Gulf of Guinea)
    const map = L.map('map', { zoomControl: true }).setView([5.30, -0.85], 8);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      maxZoom: 19, subdomains: 'abcd',
      attribution: '&copy; OpenStreetMap contributors &bull; Ghana Maritime Authority'
    }).addTo(map); setTimeout(() => { map.invalidateSize(); }, 250);

    const vesselMarkers = {};
    const sarMarkers = {};
    const logBox = document.getElementById('eventLog');

    function logEvent(text, typeClass) {
      const line = document.createElement('div');
      line.className = typeClass || 'event-info';
      line.textContent = `[${new Date().toLocaleTimeString()}] ${text}`;
      logBox.prepend(line);
    }

    function zoomSector(sec, el) {
      document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
      if (el) el.classList.add('active');

      if (sec === 'ALL') map.flyTo([5.30, -0.85], 8);
      else if (sec === 'WESTERN') map.flyTo([4.85, -2.00], 10);
      else if (sec === 'CENTRAL') map.flyTo([5.18, -1.15], 10);
      else if (sec === 'ACCRA') map.flyTo([5.62, 0.05], 10);
      else if (sec === 'VOLTA') map.flyTo([5.92, 0.98], 10);
    }

    async function initNationalGhanaBoundaries() {
      // 1. Statutory Ghana 6NM Inshore Exclusion Zone (IEZ) Polygon - Full 550km Coastline
      const fullIEZ = [
        {"lat": 5.12, "lon": -3.10}, // Half Assini (Ivory Coast border)
        {"lat": 4.87, "lon": -2.25}, // Axim
        {"lat": 4.73, "lon": -2.09}, // Cape Three Points
        {"lat": 4.88, "lon": -1.74}, // Takoradi
        {"lat": 5.08, "lon": -1.35}, // Elmina
        {"lat": 5.10, "lon": -1.24}, // Cape Coast
        {"lat": 5.35, "lon": -0.62}, // Winneba
        {"lat": 5.53, "lon": -0.21}, // Jamestown
        {"lat": 5.62, "lon": 0.01},  // Tema
        {"lat": 5.78, "lon": 0.63},  // Ada Foah
        {"lat": 5.80, "lon": 0.90},  // Cape St. Paul
        {"lat": 5.92, "lon": 0.99},  // Keta
        {"lat": 6.11, "lon": 1.20},  // Aflao (Togo border)
        // 6NM Seaward Offset Strip
        {"lat": 6.00, "lon": 1.25},
        {"lat": 5.70, "lon": 1.05},
        {"lat": 5.58, "lon": 0.65},
        {"lat": 5.45, "lon": 0.01},
        {"lat": 5.38, "lon": -0.21},
        {"lat": 5.20, "lon": -0.62},
        {"lat": 4.95, "lon": -1.24},
        {"lat": 4.75, "lon": -1.74},
        {"lat": 4.58, "lon": -2.09},
        {"lat": 4.70, "lon": -2.25},
        {"lat": 4.98, "lon": -3.10}
      ];

      L.polygon(fullIEZ.map(v => [v.lat, v.lon]), {
        color: '#f59e0b',
        fillColor: '#d97706',
        fillOpacity: 0.08,
        weight: 1.8,
        dashArray: '5, 5'
      }).addTo(map).bindPopup('<b>Ghana Inshore Exclusion Zone (IEZ)</b><br>Statutory 6NM Artisanal Reserve (Fisheries Act 625)');

      // 2. Commercial Anchorages: Tema & Takoradi
      L.circle([5.6200, 0.0500], { color: '#38bdf8', fillColor: '#0284c7', fillOpacity: 0.15, radius: 6500 }).addTo(map).bindPopup('<b>Tema Deepwater Roadstead</b><br>Crude Tankers & Container Anchorage');
      L.circle([4.8900, -1.7200], { color: '#38bdf8', fillColor: '#0284c7', fillOpacity: 0.15, radius: 6000 }).addTo(map).bindPopup('<b>Takoradi Commercial Anchorage</b><br>Offshore Oil & Bauxite Roadstead');

      // 3. International Maritime Borders
      L.polyline([[5.30, -3.10], [4.50, -3.10]], { color: '#ef4444', weight: 2, dashArray: '4, 4' }).addTo(map).bindPopup('<b>Ghana - Ivory Coast Maritime Border</b>');
      L.polyline([[6.15, 1.20], [5.30, 1.20]], { color: '#ef4444', weight: 2, dashArray: '4, 4' }).addTo(map).bindPopup('<b>Ghana - Togo Maritime Border</b>');

      // 4. Render All 20+ Coastal Cell Towers
      try {
        const tResp = await fetch('/v1/telecom/towers');
        if (tResp.ok) {
          const tData = await tResp.json();
          tData.towers.forEach(t => {
            L.circleMarker([t.lat, t.lon], {
              radius: 4,
              fillColor: '#a855f7',
              color: '#fff',
              weight: 1,
              fillOpacity: 0.9
            }).addTo(map).bindPopup(`<b>${t.name}</b><br>Tower ID: ${t.tower_id}<br>Sector: ${t.sector_type} (${t.azimuth_deg}°)<br>Region: ${t.region}`);
          });
        }
      } catch (e) {}

      await refreshRoster();
      await refreshSAR();
      await pollRadar();
    }
    initNationalGhanaBoundaries();

    async function refreshRoster() {
      try {
        const resp = await fetch('/v1/telecom/subscribers');
        if (resp.ok) {
          const data = await resp.json();
          document.getElementById('statEnrolled').innerText = data.total_subscribers;
          document.getElementById('rosterCount').innerText = `${data.total_subscribers} boats`;

          let atSea = 0;
          let onShore = 0;
          let sosCount = 0;

          if (data.subscribers.length === 0) {
            document.getElementById('fleetTable').innerHTML = '<div style="color:#64748b;padding:4px 0;">No artisanal canoes enrolled yet. Enter canoe details above to enroll.</div>';
            document.getElementById('statAtSea').innerText = '0';
            document.getElementById('statOnShore').innerText = '0';
            document.getElementById('statSOS').innerText = '0';
            return;
          }

          const rows = data.subscribers.map(s => {
            const isSea = s.status === 'AT_SEA';
            const isSOS = s.status === 'SOS_DISTRESS' || s.status === 'OVERDUE';
            if (isSea) atSea++;
            else if (isSOS) sosCount++;
            else onShore++;

            const badgeColor = isSOS ? '#dc2626' : (isSea ? '#0284c7' : '#10b981');
            return `
              <div style="border-bottom:1px solid #1e293b;padding:5px 0;display:flex;justify-content:space-between;align-items:center;">
                <div>
                  <strong style="color:#f8fafc;">${s.canoe_name}</strong> <span style="color:#94a3b8;font-size:0.68rem;">(${s.phone_number})</span><br>
                  <span style="color:#64748b;font-size:0.66rem;">${s.landing_beach} &bull; ${s.language}</span>
                </div>
                <div style="text-align:right;">
                  <span style="background:${badgeColor};color:#fff;padding:2px 6px;border-radius:3px;font-size:0.65rem;font-weight:700;">${s.status}</span>
                  <button class="btn btn-sm" style="background:#475569;margin-left:4px;padding:1px 5px;" onclick="unenrollCanoe('${s.phone_number}')">🗑️</button>
                </div>
              </div>
            `;
          }).join('');

          document.getElementById('fleetTable').innerHTML = rows;
          document.getElementById('statAtSea').innerText = atSea;
          document.getElementById('statOnShore').innerText = onShore;
          document.getElementById('statSOS').innerText = sosCount;
        }
      } catch (e) {}
    }

    async function refreshSAR() {
      try {
        const resp = await fetch('/v1/sar/incidents');
        if (resp.ok) {
          const data = await resp.json();
          if (data.incidents.length === 0) {
            document.getElementById('activeSARBox').innerHTML = '<div style="color:#64748b;">No active missing boat incidents. Coast is clear.</div>';
            return;
          }

          const html = data.incidents.map(inc => {
            const datum = inc.projected_datum;
            if (!sarMarkers[inc.incident_id]) {
              const marker = L.circleMarker([datum.lat, datum.lon], {
                radius: 9,
                fillColor: '#dc2626',
                color: '#fff',
                weight: 2,
                fillOpacity: 0.95
              }).addTo(map).bindPopup(`<b>MISSING CANOE: ${inc.canoe_name}</b><br>Owner: ${inc.phone_number}<br>Beach: ${inc.landing_beach}<br><b>Guinea Current Datum: [${datum.lat.toFixed(4)}, ${datum.lon.toFixed(4)}]</b><br>Drifted ${inc.drift_distance_nm} NM Eastward<br>Search Radius: ${inc.search_radius_nm} NM`);

              const circle = L.circle([datum.lat, datum.lon], {
                radius: inc.search_radius_nm * 1852,
                color: '#ef4444',
                fillColor: '#dc2626',
                fillOpacity: 0.12,
                dashArray: '5, 5'
              }).addTo(map);

              sarMarkers[inc.incident_id] = { marker, circle };
            }

            return `
              <div style="border-bottom:1px solid #334155;padding:4px 0;display:flex;justify-content:space-between;align-items:center;">
                <div>
                  <strong style="color:#f87171;">🚨 ${inc.canoe_name}</strong> <span style="font-size:0.68rem;color:#94a3b8;">(${inc.landing_beach})</span><br>
                  <span style="font-size:0.66rem;color:#fbbf24;">Datum: [${datum.lat.toFixed(3)}, ${datum.lon.toFixed(3)}] &bull; ${inc.drift_distance_nm} NM adrift</span>
                </div>
                <button class="btn btn-sm" style="background:#059669;padding:2px 6px;" onclick="resolveSAR('${inc.incident_id}')">Found</button>
              </div>
            `;
          }).join('');
          document.getElementById('activeSARBox').innerHTML = html;
        }
      } catch (e) {}
    }

    async function pollRadar() {
      try {
        const resp = await fetch('/v1/fleet/snapshot');
        if (resp.ok) {
          const snap = await resp.json();
          snap.vehicles.forEach(v => {
            const isCanoe = v.entity_id.startsWith('canoe-');
            const color = isCanoe ? '#10b981' : '#38bdf8';
            const radius = isCanoe ? 7 : 9;

            if (vesselMarkers[v.entity_id]) {
              vesselMarkers[v.entity_id].setLatLng([v.position.lat, v.position.lon]);
            } else {
              vesselMarkers[v.entity_id] = L.circleMarker([v.position.lat, v.position.lon], {
                radius: radius,
                fillColor: color,
                color: '#fff',
                weight: 2,
                fillOpacity: 0.95
              }).addTo(map).bindPopup(`<b>${v.entity_id}</b><br>Pos: [${v.position.lat.toFixed(4)}, ${v.position.lon.toFixed(4)}]<br>Speed: ${v.kinematics.velocity_mps.toFixed(1)} m/s`);
            }
          });
        }
      } catch (e) {}
    }

    setInterval(pollRadar, 3000);
    setInterval(refreshRoster, 5000);
    setInterval(refreshSAR, 6000);

    async function enrollCanoe() {
      const phone = document.getElementById('regPhone').value.trim();
      const canoe = document.getElementById('regCanoe').value.trim();
      const beach = document.getElementById('regBeach').value;
      const lang = document.getElementById('regLang').value;
      if (!phone || !canoe) {
        alert('Please enter both mobile number and canoe name.');
        return;
      }
      const resp = await fetch('/v1/telecom/register', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ phone_number: phone, canoe_name: canoe, landing_beach: beach, language: lang })
      });
      if (resp.ok) {
        logEvent(`[ENROLLMENT SUCCESS] Registered ${canoe} (${phone}) at ${beach}. Zero daily dials required.`, 'event-enter');
        document.getElementById('regPhone').value = '';
        document.getElementById('regCanoe').value = '';
        await refreshRoster();
      }
    }

    async function unenrollCanoe(phone) {
      if (confirm(`Remove canoe with phone ${phone}?`)) {
        await fetch(`/v1/telecom/subscribers/${phone}`, { method: 'DELETE' });
        logEvent(`[UNENROLLED] Removed subscriber ${phone}.`, 'event-info');
        await refreshRoster();
      }
    }

    async function reportMissingBoat() {
      const query = document.getElementById('sarInput').value.trim();
      if (!query) {
        alert('Enter a phone number or canoe name to report missing.');
        return;
      }
      // Check if phone or match name
      const subsResp = await fetch('/v1/telecom/subscribers');
      const subsData = await subsResp.json();
      let match = subsData.subscribers.find(s => s.phone_number.includes(query) || s.canoe_name.toLowerCase().includes(query.toLowerCase()));
      const targetPhone = match ? match.phone_number : query;

      logEvent(`[SAR REPORTED] Declaring emergency search & rescue for ${query}...`, 'event-sar');
      const resp = await fetch('/v1/sar/declare', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ phone_number: targetPhone, reason: 'FAMILY_REPORTED_OVERDUE_AT_SEA', hours_adrift: 6.0 })
      });
      if (resp.ok) {
        const data = await resp.json();
        logEvent(`🚨 [SAR DOCKET CREATED]: Datum [${data.projected_datum.lat}, ${data.projected_datum.lon}]. Guinea Current carried canoe ${data.drift_distance_nm} NM east. Ghana Navy notified!`, 'event-sar');
        document.getElementById('sarInput').value = '';
        await refreshSAR();
        await refreshRoster();
      } else {
        alert('Could not find registered boat with that number. Please ensure the canoe is registered.');
      }
    }

    async function resolveSAR(incId) {
      await fetch(`/v1/sar/resolve/${incId}`, { method: 'POST' });
      if (sarMarkers[incId]) {
        map.removeLayer(sarMarkers[incId].marker);
        map.removeLayer(sarMarkers[incId].circle);
        delete sarMarkers[incId];
      }
      logEvent(`[SAR RESOLVED] Incident ${incId} marked safely recovered.`, 'event-enter');
      await refreshSAR();
      await refreshRoster();
    }

    async function simulateKeypadSOS() {
      const subsResp = await fetch('/v1/telecom/subscribers');
      const subsData = await subsResp.json();
      const phone = subsData.subscribers.length > 0 ? subsData.subscribers[0].phone_number : '+233244111222';
      logEvent(`[KEYPAD MAYDAY] Incoming USSD/SMS SOS from ${phone}...`, 'event-cpa');
      const resp = await fetch('/v1/telecom/sos', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ phone_number: phone, message: 'MAYDAY_ENGINE_DEAD_SURF_HIGH' })
      });
      const data = await resp.json();
      logEvent(`🚨 [EMERGENCY MAYDAY ACTIVATED]: ${data.canoe_name} triggered SOS! Navy rescue docket created.`, 'event-cpa');
      await refreshSAR();
      await refreshRoster();
    }

    async function simulateSeawardTrip() {
      const subsResp = await fetch('/v1/telecom/subscribers');
      const subsData = await subsResp.json();
      const phone = subsData.subscribers.length > 0 ? subsData.subscribers[0].phone_number : '+233244111222';
      logEvent(`[PASSIVE TRIPWIRE] Canoe ${phone} handover to Seaward Tower (TA=12)...`, 'event-info');
      const resp = await fetch('/v1/telecom/cell-event', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ phone_number: phone, tower_id: 'TOWER_KETA_SEAWARD', timing_advance: 12 })
      });
      const data = await resp.json();
      logEvent(`[AUTO SEA LAUNCH] ${data.canoe_name} marked AT_SEA (${data.distance_offshore_km} km out) without touching phone!`, 'event-enter');
      await refreshRoster();
      await pollRadar();
    }

    async function injectAISVessel() {
      logEvent('[AIS INGESTION] Ingesting Class-A crude tanker NMEA radio sentence...', 'event-info');
      const sample = '!AIVDM,1,1,,A,19Eu8C002A00>aP3=kp9TWb00000,0*64';
      const resp = await fetch('/v1/ais/nmea', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ raw_nmea: sample })
      });
      const data = await resp.json();
      logEvent(`[AIS TANKER MAPPED] MMSI ${data.mmsi} Speed: ${data.speed_knots}kt Heading: ${data.heading_deg}°`, 'event-info');
      await pollRadar();
    }

    async function clearRadar() {
      await fetch('/v1/fleet/clear', { method: 'POST' });
      for (const id in vesselMarkers) {
        map.removeLayer(vesselMarkers[id]);
        delete vesselMarkers[id];
      }
      logEvent('[RADAR RESET] Test vessels cleared from active display.', 'event-info');
    }
  </script>
</body>
</html>
"""
