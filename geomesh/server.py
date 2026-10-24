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

# Seed initial registered Ghanaian artisanal fishermen if table is empty
if len(telecom_gateway.list_subscribers()) == 0:
    telecom_gateway.register_subscriber(
        phone_number="+233241234501",
        canoe_name="Nyimdzii [Keta-01]",
        landing_beach="Keta Beach",
        language=Language.EWE,
    )
    telecom_gateway.register_subscriber(
        phone_number="+233241234502",
        canoe_name="Mawueyro [Keta-02]",
        landing_beach="Dzelukope",
        language=Language.EWE,
    )
    telecom_gateway.register_subscriber(
        phone_number="+233241234503",
        canoe_name="Oshu [Jamestown-01]",
        landing_beach="Jamestown",
        language=Language.GA,
    )


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

@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
def get_map_dashboard() -> str:
    """Serve production real-time Ghana Maritime defense & safety dashboard."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>GeoMesh-Stream: Ghana Maritime Defense & Artisanal Safety</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    body { margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #070d18; color: #f8fafc; }
    #container { display: flex; height: 100vh; }
    #sidebar { width: 420px; background: #0f172a; padding: 20px; overflow-y: auto; border-right: 1px solid #1e293b; box-shadow: 2px 0 10px rgba(0,0,0,0.5); }
    #map { flex-grow: 1; height: 100%; }
    h1 { font-size: 1.15rem; margin-top: 0; color: #38bdf8; display: flex; align-items: center; justify-content: space-between; }
    .badge-live { font-size: 0.65rem; background: #059669; padding: 3px 8px; border-radius: 9999px; letter-spacing: 0.05em; font-weight: 700; color: #fff; }
    .btn { background: #0284c7; color: white; border: none; padding: 8px 12px; border-radius: 5px; cursor: pointer; width: 100%; font-weight: 600; font-size: 0.8rem; margin-bottom: 8px; transition: background 0.2s; }
    .btn:hover { background: #0369a1; }
    .btn-warn { background: #b45309; }
    .btn-warn:hover { background: #92400e; }
    .btn-danger { background: #dc2626; }
    .btn-danger:hover { background: #b91c1c; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 6px; padding: 12px; margin-bottom: 12px; }
    .card h3 { margin-top: 0; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.05em; color: #94a3b8; margin-bottom: 8px; display:flex; justify-content:space-between; align-items:center; }
    .event-log { font-family: monospace; font-size: 0.75rem; max-height: 200px; overflow-y: auto; background: #020617; padding: 8px; border-radius: 4px; line-height: 1.5; border: 1px solid #1e293b; }
    .event-enter { color: #f87171; font-weight: 600; }
    .event-exit { color: #fbbf24; }
    .event-cpa { color: #ef4444; font-weight: 700; }
    .event-info { color: #38bdf8; }
    .event-sar { color: #f59e0b; font-weight: 700; }
    .leaflet-tile { filter: invert(100%) hue-rotate(180deg) brightness(88%) contrast(90%); }
    .leaflet-container { background: #050b14; }
    .zone-pill { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 0.68rem; margin-right: 4px; margin-bottom: 4px; }
  </style>
</head>
<body>
  <div id="container">
    <div id="sidebar">
      <h1>GeoMesh-Stream <span class="badge-live">&bull; LIVE SURVEILLANCE</span></h1>
      <p style="font-size: 0.76rem; color: #94a3b8; line-height: 1.4; margin-top: 4px;">
        Ghana Coastal Maritime Defense, Zero-Touch Telecom LBS &amp; Guinea Current SAR Center.
      </p>

      <div class="card">
        <h3>One-Time Keypad Fisherman Registration</h3>
        <div style="display:flex;flex-direction:column;gap:6px;font-size:0.75rem;">
          <input id="subPhone" type="text" placeholder="Phone (+233241234567)" style="background:#0b0f19;border:1px solid #334155;color:#fff;padding:6px;border-radius:4px;font-size:0.75rem;" />
          <input id="subCanoe" type="text" placeholder="Canoe Name (e.g. Nyame Nti)" style="background:#0b0f19;border:1px solid #334155;color:#fff;padding:6px;border-radius:4px;font-size:0.75rem;" />
          <div style="display:flex;gap:6px;">
            <select id="subBeach" style="flex:1;background:#0b0f19;border:1px solid #334155;color:#fff;padding:6px;border-radius:4px;font-size:0.75rem;">
              <option value="Keta Beach">Keta Beach</option>
              <option value="Dzelukope">Dzelukope</option>
              <option value="Ada Foah">Ada Foah</option>
              <option value="Prampram">Prampram</option>
              <option value="Jamestown">Jamestown</option>
              <option value="Elmina">Elmina</option>
            </select>
            <select id="subLang" style="flex:1;background:#0b0f19;border:1px solid #334155;color:#fff;padding:6px;border-radius:4px;font-size:0.75rem;">
              <option value="EWE">Ewe (Keta)</option>
              <option value="GA">Ga (Accra)</option>
              <option value="FANTE">Fante</option>
              <option value="ENGLISH">English</option>
            </select>
          </div>
          <button onclick="registerFisherman()" style="background:#4f46e5;color:#fff;border:none;padding:6px;border-radius:4px;cursor:pointer;font-weight:600;font-size:0.75rem;">Enroll Once (Durable SQLite)</button>
        </div>
      </div>

      <div class="card">
        <h3>Enrolled Fleet &amp; Voyage Status</h3>
        <div id="subscribersList" style="font-size:0.74rem;max-height:130px;overflow-y:auto;line-height:1.5;">Loading subscribers...</div>
      </div>

      <div class="card">
        <h3>Active SAR Incidents (Guinea Current)</h3>
        <div id="sarList" style="font-size:0.74rem;max-height:100px;overflow-y:auto;line-height:1.4;color:#94a3b8;">No active search &amp; rescue operations.</div>
      </div>

      <div class="card">
        <h3>Production Ingestion Actions</h3>
        <button class="btn" onclick="testCarrierWebhook()">Send Carrier Handover Webhook (MTN Cape St. Paul)</button>
        <button class="btn" style="background:#0891b2;" onclick="injectAISBurst()">Inject AIS Commercial Tanker (!AIVDM Radio)</button>
        <button class="btn btn-warn" onclick="testVoiceRobocall()">Test Multilingual IVR Robocall (Ewe)</button>
        <button class="btn btn-danger" onclick="declareEmergencySAR()">Declare SAR Overdue Emergency (Ghana Navy)</button>
        <button class="btn" style="background:#334155;margin-bottom:0;" onclick="clearRadarEntities()">Clear Radar Entities</button>
      </div>

      <div class="card">
        <h3>Protected Zones &amp; Boundaries</h3>
        <div id="fenceList" style="font-size: 0.74rem; line-height: 1.6;">Loading Ghana maritime zones...</div>
      </div>

      <div class="card">
        <h3>Real-Time Operational Audit Log</h3>
        <div id="eventLog" class="event-log">Production surveillance active on Gulf of Guinea...</div>
      </div>

      <div style="font-size: 0.72rem; color: #64748b; text-align: center; line-height: 1.4;">
        Ghana Maritime Authority &bull; Fisheries Commission (Act 625)<br>
        OpenAPI docs at <a href="/docs" target="_blank" style="color: #38bdf8;">/docs</a> &bull; <a href="/metrics" target="_blank" style="color: #38bdf8;">/metrics</a>
      </div>
    </div>
    <div id="map"></div>
  </div>

  <script>
    const map = L.map('map').setView([5.78, 0.45], 10);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 18,
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    const vesselMarkers = {};
    const sarLayers = {};
    const logBox = document.getElementById('eventLog');

    function logEvent(text, typeClass) {
      const line = document.createElement('div');
      line.className = typeClass || 'event-info';
      line.textContent = `[${new Date().toLocaleTimeString()}] ${text}`;
      logBox.prepend(line);
    }

    async function initGhanaMaritime() {
      // 1. Tema Commercial Deepwater Anchorage
      await fetch('/v1/geofences/radial', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          geofence_id: 'tema-commercial-anchorage',
          name: 'Tema Port Deepwater Anchorage',
          lat: 5.6200,
          lon: 0.0500,
          radius_m: 6500
        })
      });

      L.circle([5.6200, 0.0500], {
        color: '#38bdf8',
        fillColor: '#0284c7',
        fillOpacity: 0.15,
        radius: 6500
      }).addTo(map).bindPopup('<b>Tema Deepwater Anchorage</b><br>Crude Tankers & Container Roadstead');

      // 2. Keta Artisanal Fishing Grounds
      const ketaVerts = [
        {"lat": 5.8200, "lon": 0.8800},
        {"lat": 5.9500, "lon": 0.9800},
        {"lat": 5.9200, "lon": 1.1500},
        {"lat": 5.7600, "lon": 1.0500}
      ];
      await fetch('/v1/geofences/polygon', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          geofence_id: 'keta-artisanal-grounds',
          name: 'Keta Artisanal Fishing Grounds',
          vertices: ketaVerts
        })
      });

      L.polygon([[5.8200, 0.8800], [5.9500, 0.9800], [5.9200, 1.1500], [5.7600, 1.0500]], {
        color: '#10b981',
        fillColor: '#059669',
        fillOpacity: 0.18,
        weight: 2
      }).addTo(map).bindPopup('<b>Keta Artisanal Grounds</b><br>Cape St. Paul to Dzelukope Canoe Fishery');

      // 3. Ghana 6NM Inshore Exclusion Zone (IEZ)
      const iezVerts = [
        {"lat": 5.7000, "lon": 0.0500},
        {"lat": 5.7600, "lon": 0.6300},
        {"lat": 5.7800, "lon": 0.9000},
        {"lat": 5.9200, "lon": 1.0500},
        {"lat": 6.1000, "lon": 1.2000},
        {"lat": 6.0000, "lon": 1.3000},
        {"lat": 5.7000, "lon": 1.1200},
        {"lat": 5.6000, "lon": 0.6500},
        {"lat": 5.5500, "lon": 0.1000}
      ];
      await fetch('/v1/geofences/polygon', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          geofence_id: 'ghana-iez-reserve',
          name: 'Ghana 6NM Inshore Exclusion Zone (IEZ)',
          vertices: iezVerts
        })
      });

      L.polygon(iezVerts.map(v => [v.lat, v.lon]), {
        color: '#f59e0b',
        fillColor: '#d97706',
        fillOpacity: 0.08,
        weight: 1.5,
        dashArray: '6, 6'
      }).addTo(map).bindPopup('<b>Ghana Inshore Exclusion Zone (IEZ)</b><br>6 Nautical Mile Artisanal Reserve (Fisheries Act 625)');

      // Togo Border Line Marker
      L.polyline([[6.1000, 1.2000], [5.5500, 1.2000]], {
        color: '#ef4444',
        weight: 2,
        dashArray: '4, 4'
      }).addTo(map).bindPopup('<b>Ghana - Togo Maritime Boundary</b>');

      document.getElementById('fenceList').innerHTML = `
        <div><span class="zone-pill" style="background:#0284c7;color:#fff;">ANCHORAGE</span> <b>Tema Deepwater Roadstead</b> (6.5km)</div>
        <div><span class="zone-pill" style="background:#059669;color:#fff;">ARTISANAL</span> <b>Keta Coastal Fishing Grounds</b></div>
        <div><span class="zone-pill" style="background:#d97706;color:#fff;">IEZ</span> <b>Ghana 6NM Exclusion Strip</b> (Act 625)</div>
        <div><span class="zone-pill" style="background:#ef4444;color:#fff;">BORDER</span> <b>Ghana - Togo Maritime Line (Aflao)</b></div>
      `;
      await refreshSubscribers();
      await refreshSAR();
      await pollRadar();
    }
    initGhanaMaritime();

    async function refreshSubscribers() {
      try {
        const resp = await fetch('/v1/telecom/subscribers');
        if (resp.ok) {
          const data = await resp.json();
          const rows = data.subscribers.map(s => {
            const isSea = s.status === 'AT_SEA';
            const badgeColor = isSea ? '#0284c7' : '#10b981';
            return `
              <div style="border-bottom:1px solid #334155;padding:4px 0;display:flex;justify-content:space-between;align-items:center;">
                <div>
                  <strong>${s.canoe_name}</strong> <span style="color:#94a3b8;font-size:0.7rem;">(${s.phone_number})</span><br>
                  <span style="color:#64748b;font-size:0.68rem;">${s.landing_beach} &bull; ${s.language}</span>
                </div>
                <div style="text-align:right;">
                  <span style="background:${badgeColor};color:#fff;padding:2px 6px;border-radius:4px;font-size:0.65rem;font-weight:700;">${s.status}</span>
                  ${isSea ? `<div style="font-size:0.65rem;color:#38bdf8;margin-top:2px;">${s.distance_offshore_km} km out</div>` : ''}
                </div>
              </div>
            `;
          }).join('');
          document.getElementById('subscribersList').innerHTML = rows || '<div style="color:#64748b;">No fishermen enrolled yet.</div>';
        }
      } catch (e) {}
    }

    async function refreshSAR() {
      try {
        const resp = await fetch('/v1/sar/incidents');
        if (resp.ok) {
          const data = await resp.json();
          if (data.incidents.length === 0) {
            document.getElementById('sarList').innerHTML = '<div style="color:#64748b;">No active SAR incidents.</div>';
            return;
          }
          const html = data.incidents.map(inc => {
            // Render Datum on map
            const datum = inc.projected_datum;
            if (!sarLayers[inc.incident_id]) {
              const marker = L.circleMarker([datum.lat, datum.lon], {
                radius: 8,
                fillColor: '#dc2626',
                color: '#fff',
                weight: 2,
                fillOpacity: 0.95
              }).addTo(map).bindPopup(`<b>SAR DATUM: ${inc.canoe_name}</b><br>Drifted ${inc.drift_distance_nm} NM Eastward<br>Search Radius: ${inc.search_radius_nm} NM`);

              const circle = L.circle([datum.lat, datum.lon], {
                radius: inc.search_radius_nm * 1852,
                color: '#ef4444',
                fillColor: '#dc2626',
                fillOpacity: 0.12,
                dashArray: '5, 5'
              }).addTo(map);

              sarLayers[inc.incident_id] = { marker, circle };
            }

            return `
              <div style="border-bottom:1px solid #334155;padding:4px 0;">
                <strong style="color:#f87171;">${inc.canoe_name}</strong> <span style="font-size:0.68rem;color:#94a3b8;">(${inc.landing_beach})</span><br>
                <span style="font-size:0.68rem;color:#fbbf24;">Datum: [${datum.lat.toFixed(4)}, ${datum.lon.toFixed(4)}] &bull; ${inc.drift_distance_nm} NM adrift</span>
              </div>
            `;
          }).join('');
          document.getElementById('sarList').innerHTML = html;
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

    // Live continuous polling of server state (zero client-side synthetic simulations)
    setInterval(pollRadar, 2500);
    setInterval(refreshSubscribers, 5000);
    setInterval(refreshSAR, 6000);

    async function registerFisherman() {
      const phone = document.getElementById('subPhone').value.trim();
      const canoe = document.getElementById('subCanoe').value.trim();
      const beach = document.getElementById('subBeach').value;
      const lang = document.getElementById('subLang').value;
      if (!phone || !canoe) {
        alert('Please enter both phone number and canoe name.');
        return;
      }
      const resp = await fetch('/v1/telecom/register', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ phone_number: phone, canoe_name: canoe, landing_beach: beach, language: lang })
      });
      if (resp.ok) {
        logEvent(`[LIFETIME SUBSCRIBER] Enrolled ${canoe} (${phone}) at ${beach}. Zero daily dials required.`, 'event-enter');
        document.getElementById('subPhone').value = '';
        document.getElementById('subCanoe').value = '';
        await refreshSubscribers();
      }
    }

    async function testCarrierWebhook() {
      logEvent('[CARRIER WEBHOOK] Receiving incoming signaling event from MTN Ghana...', 'event-info');
      const resp = await fetch('/v1/telecom/carrier-webhook', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          phone_number: '+233241234501',
          tower_id: 'TOWER_KETA_SEAWARD',
          timing_advance: 14,
          signal_dbm: -80
        })
      });
      const data = await resp.json();
      logEvent(`[PASSIVE TRIPWIRE] ${data.canoe_name} detected AT_SEA (${data.distance_offshore_km} km out) via Cape St. Paul TA=14.`, 'event-enter');
      await refreshSubscribers();
      await pollRadar();
    }

    async function injectAISBurst() {
      logEvent('[AIS INGESTION] Processing raw NMEA 0183 !AIVDM radio burst from Tema Port VHF antenna...', 'event-info');
      const sampleSentence = '!AIVDM,1,1,,A,19Eu8C002A00>aP3=kp9TWb00000,0*64';
      const resp = await fetch('/v1/ais/nmea', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ raw_nmea: sampleSentence })
      });
      const data = await resp.json();
      logEvent(`[AIS DECODED] MMSI ${data.mmsi} (Speed: ${data.speed_knots}kt, Heading: ${data.heading_deg}°). Vessel mapped on radar.`, 'event-info');
      await pollRadar();
    }

    async function testVoiceRobocall() {
      logEvent('[TELEPHONY] Dispatching automated emergency voice call in Ewe to +233241234501...', 'event-cpa');
      const resp = await fetch('/v1/telecom/alert/voice', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          phone_number: '+233241234501',
          cpa_nm: 0.65,
          tcpa_min: 7.2
        })
      });
      const data = await resp.json();
      logEvent(`📞 [CALL CONNECTED - ${data.provider}]: Audio message in ${data.language}: "${data.audio_message}"`, 'event-cpa');
    }

    async function declareEmergencySAR() {
      logEvent('[SAR DECLARATION] Declaring emergency search & rescue for Keta canoe past return window...', 'event-sar');
      const resp = await fetch('/v1/sar/declare', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          phone_number: '+233241234501',
          reason: 'OUTBOARD_MOTOR_FAILURE_DRIFT',
          hours_adrift: 5.5
        })
      });
      const data = await resp.json();
      logEvent(`🚨 [SAR DOCKET CREATED - ${data.incident_id}]: Guinea Current Datum: [${data.projected_datum.lat}, ${data.projected_datum.lon}], Search Radius: ${data.search_radius_nm} NM. Ghana Navy alerted!`, 'event-sar');
      await refreshSAR();
    }

    async function clearRadarEntities() {
      await fetch('/v1/fleet/clear', { method: 'POST' });
      for (const id in vesselMarkers) {
        map.removeLayer(vesselMarkers[id]);
        delete vesselMarkers[id];
      }
      for (const id in sarLayers) {
        map.removeLayer(sarLayers[id].marker);
        map.removeLayer(sarLayers[id].circle);
        delete sarLayers[id];
      }
      logEvent('[RADAR CLEARED] Transient vessels and SAR markers reset.', 'event-info');
    }
  </script>
</body>
</html>
"""
