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
    normalize_ghana_phone,
)
from geomesh.opencellid import get_opencellid_manager
from geomesh.probe import ProbeStatus, get_probe_engine
from geomesh.sdr_finder import (
    SDRBand,
    SDRCellularPayload,
    SDRScanRequest,
    SDRTargetFix,
    SDRTargetStatus,
    sdr_payload,
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
probe_engine = get_probe_engine()
opencellid_mgr = get_opencellid_manager()

from geomesh.live_ais import LiveAISStreamClient
live_ais_client = LiveAISStreamClient(engine=_engine)

_telemetry_pings_total = 0
_geofence_breaches_total = 0


@app.on_event("startup")
async def startup_event():
    live_ais_client.start()


@app.on_event("shutdown")
async def shutdown_event():
    live_ais_client.stop()

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


class AISVesselInjectRequest(BaseModel):
    mmsi: int = Field(..., description="Vessel MMSI 9-digit identifier")
    vessel_name: str = Field(default="COMMERCIAL TANKER", description="Vessel name or callsign")
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    speed_knots: float = Field(default=16.5, ge=0.0, description="Speed over ground in knots")
    heading_deg: float = Field(default=350.0, ge=0.0, le=360.0, description="Heading in degrees 0..360")


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

        status_str = "UNDERWAY"
        if ent_id.startswith("canoe-"):
            clean_phone = ent_id.replace("canoe-", "")
            sub = telecom_gateway.get_subscriber(clean_phone)
            if sub:
                status_str = sub.status.value if hasattr(sub.status, "value") else str(sub.status)

        meta = live_ais_client.get_vessel_metadata(ent_id)
        ship_name = meta["ship_name"] if meta else (ent_id if not ent_id.startswith("canoe-") else None)
        is_live = meta is not None

        vehicles.append({
            "entity_id": ent_id,
            "ship_name": ship_name,
            "is_live": is_live,
            "position": {"lat": pos.lat, "lon": pos.lon},
            "kinematics": {
                "velocity_mps": k_state.velocity_mps if k_state else 0.0,
                "bearing_deg": k_state.bearing_deg if k_state else 0.0,
                "acceleration_mps2": k_state.acceleration_mps2 if k_state else 0.0,
            },
            "speed_mps": k_state.velocity_mps if k_state else 0.0,
            "heading_deg": k_state.bearing_deg if k_state else 0.0,
            "status": status_str,
            "predicted_position_30s": {"lat": pred_30s.lat, "lon": pred_30s.lon} if pred_30s else None,
            "active_geofences": active_fences,
        })

    hazards = []
    for v in vehicles:
        if v["entity_id"].startswith("canoe-"):
            h_list = check_collision_risks(engine, v["entity_id"])
            if h_list:
                hazards.extend(h_list)

    return {
        "snapshot_id": snapshot_id,
        "timestamp_ns": time.time_ns(),
        "total_tracked_vehicles": len(vehicles),
        "live_ais_status": live_ais_client.status,
        "live_ais_messages": live_ais_client.total_messages_received,
        "live_vessels_count": live_ais_client.total_vessels_tracked,
        "vehicles": vehicles,
        "collision_hazards": hazards,
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


def check_collision_risks(engine: GeoMeshEngine, vessel_id: str) -> List[Dict[str, Any]]:
    """
    Evaluate ARPA collision risk (CPA/TCPA) between vessel_id and nearby vessels.
    If a commercial tanker and an artisanal canoe are on a collision course (CPA <= 1.0 NM, TCPA <= 20 min),
    automatically dispatch an audible mother-tongue robocall to the canoe.
    """
    hazards = []
    state = engine.get_kinematic_state(vessel_id)
    if not state:
        return hazards

    nearby = engine.query_radius(state.last_point, radius_m=9260.0)
    for other_id, pt, dist_m in nearby:
        if other_id == vessel_id:
            continue
        is_pair = (vessel_id.startswith("canoe-") and not other_id.startswith("canoe-")) or \
                  (not vessel_id.startswith("canoe-") and other_id.startswith("canoe-"))
        if not is_pair:
            continue

        cpa_res = engine.calculate_cpa_tcpa(vessel_id, other_id)
        if not cpa_res:
            continue
        cpa_m, tcpa_s = cpa_res
        cpa_nm = round(cpa_m / 1852.0, 2)
        tcpa_min = round(tcpa_s / 60.0, 1)

        if cpa_nm <= 1.0 and 0.0 < tcpa_min <= 20.0:
            canoe_id = vessel_id if vessel_id.startswith("canoe-") else other_id
            threat_id = other_id if vessel_id.startswith("canoe-") else vessel_id
            clean_phone = canoe_id.replace("canoe-", "")
            sub = telecom_gateway.get_subscriber(clean_phone)
            call_res = None
            if sub:
                call_res = telecom_gateway._voice_service.dispatch_alert_call(
                    subscriber=sub,
                    threat_vessel=f"Commercial Vessel ({threat_id})",
                    cpa_nm=cpa_nm,
                    tcpa_min=tcpa_min,
                )
            hazards.append({
                "threat_vessel": threat_id,
                "canoe_id": canoe_id,
                "canoe_name": sub.canoe_name if sub else "Artisanal Canoe",
                "phone_number": clean_phone,
                "language": str(sub.language) if sub else "EWE",
                "cpa_nm": cpa_nm,
                "tcpa_minutes": tcpa_min,
                "distance_m": round(dist_m, 1),
                "robocall_dispatched": True,
                "voice_script": (call_res.get("audio_message") or call_res.get("voice_script") or "") if call_res else "",
            })
    return hazards


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



@app.get("/v1/telecom/locate/{phone_number}", tags=["Telecom LBS & Artisanal Registry"])
def locate_canoe_realtime(phone_number: str) -> Dict[str, Any]:
    """
    Real-time query to locate an artisanal canoe via GSM network signaling.
    Answers questions like: 'Someone left Keta and went to Vodza to sell fish, are they missing?'
    Calculates distance from home landing beach and confirms whether safe on shore or active at sea.
    """
    try:
        return telecom_gateway.locate_subscriber(phone_number)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Artisanal canoe with identifier '{phone_number}' not found in national registry."
        )


@app.post("/v1/telecom/ping/{phone_number}", tags=["Telecom LBS & Artisanal Registry"])
def ping_canoe_radio(
    phone_number: str,
    simulated_tower: Optional[str] = Query(None),
    timing_advance: int = Query(0),
) -> Dict[str, Any]:
    """
    Trigger active GSM radio paging / HLR probe for an artisanal canoe.
    Simulates or executes real-time tower handover (e.g. Relocating from Keta to Vodza).
    """
    try:
        loc = telecom_gateway.ping_radio(
            phone_number=phone_number,
            simulated_tower=simulated_tower,
            timing_advance=timing_advance,
        )
        if simulated_tower:
            pos = loc["current_coordinates"]
            ping = TelemetryPing.create(
                entity_id=f"canoe-{loc['phone_number']}",
                lat=pos["lat"],
                lon=pos["lon"],
                speed_mps=1.8 if loc["current_status"] == "AT_SEA" else 0.0,
                heading_deg=140.0,
                timestamp_ns=time.time_ns(),
            )
            engine = get_engine()
            engine.ingest_telemetry(ping)
        return loc
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Artisanal canoe with identifier '{phone_number}' not found in national registry."
        )


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
    res["collision_hazards"] = check_collision_risks(engine, ping.entity_id)
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


@app.get("/v1/ais/live/status", tags=["AIS Commercial Ship Telemetry"])
def get_live_ais_status() -> Dict[str, Any]:
    """Get status and live statistics from AISStream.io real-time satellite & coastal feed."""
    return {
        "status": live_ais_client.status,
        "api_key_configured": bool(live_ais_client.api_key),
        "total_messages_received": live_ais_client.total_messages_received,
        "total_vessels_tracked": live_ais_client.total_vessels_tracked,
        "last_connected_time": live_ais_client.last_connected_time,
        "last_message_time": live_ais_client.last_message_time,
        "tracked_vessels": list(live_ais_client._vessel_metadata.values()),
    }


@app.post("/v1/ais/live/start", tags=["AIS Commercial Ship Telemetry"])
def start_live_ais() -> Dict[str, Any]:
    """Start live background AIS stream from AISStream.io."""
    live_ais_client.start()
    return {"started": True, "status": live_ais_client.status}


@app.post("/v1/ais/live/stop", tags=["AIS Commercial Ship Telemetry"])
def stop_live_ais() -> Dict[str, Any]:
    """Stop live background AIS stream from AISStream.io."""
    live_ais_client.stop()
    return {"stopped": True, "status": live_ais_client.status}


@app.post("/v1/ais/vessel/inject", tags=["AIS Commercial Ship Telemetry"])
def inject_ais_vessel(req: AISVesselInjectRequest) -> Dict[str, Any]:
    """Directly inject an AIS commercial vessel into the spatial mesh for collision avoidance testing."""
    engine = get_engine()
    speed_mps = req.speed_knots * 0.514444
    ping = TelemetryPing.create(
        entity_id=f"MMSI-{req.mmsi}",
        lat=req.lat,
        lon=req.lon,
        speed_mps=speed_mps,
        heading_deg=req.heading_deg,
        timestamp_ns=time.time_ns(),
    )
    alerts = engine.ingest_telemetry(ping)
    hazards = check_collision_risks(engine, ping.entity_id)
    return {
        "injected": True,
        "mmsi": req.mmsi,
        "vessel_name": req.vessel_name,
        "entity_id": ping.entity_id,
        "lat": req.lat,
        "lon": req.lon,
        "speed_knots": req.speed_knots,
        "heading_deg": req.heading_deg,
        "alerts": [{"event": str(a.event), "geofence_id": a.geofence_id} for a in alerts],
        "hazards": hazards,
    }


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
    inc = durable_store.get_sar_incident(incident_id)
    success = durable_store.resolve_sar_incident(incident_id, "RESOLVED")
    if not success:
        raise HTTPException(status_code=404, detail=f"SAR incident '{incident_id}' not found.")
    if inc:
        sub = telecom_gateway.get_subscriber(inc.phone_number)
        if sub:
            sub.status = VesselStatus.ON_SHORE
            sub.sea_departure_ts_ns = None
            if durable_store:
                durable_store.close_voyage(inc.phone_number)
                rec = durable_store.get_subscriber(inc.phone_number)
                if rec:
                    rec.status = "ON_SHORE"
                    durable_store.save_subscriber(rec)
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
    engine = get_engine()
    engine.remove_entity(f"canoe-{phone_number}")
    canonical = normalize_ghana_phone(phone_number)
    if canonical != phone_number:
        engine.remove_entity(f"canoe-{canonical}")
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


# -------------------------------------------------------------------------
# Self-Hosted Silent Radio Prober & Zero-Cost Signaling Endpoints
# -------------------------------------------------------------------------

class SilentProbeRequest(BaseModel):
    phone_number: str


@app.post("/v1/probe/silent-ping", tags=["Self-Hosted Radio Probing & Watchdog"])
def execute_silent_probe(req: SilentProbeRequest) -> Dict[str, Any]:
    """
    Execute a zero-cost sub-second SIP ring-and-drop probe.
    If the phone returns 480 Absent / Water Immersion, automatically triggers
    the Guinea Current oceanographic drift model to project emergency search datum.
    """
    res = probe_engine.probe_subscriber(req.phone_number)
    sar_triggered = False
    incident_id = None
    drift_datum = None

    if res.status == ProbeStatus.IMPLICIT_DETACH_ABSENT:
        try:
            incident = sar_watchdog.declare_incident(
                phone_number=res.phone_number,
                reason="SILENT_PROBE_IMPLICIT_DETACH_WATER_IMMERSION",
                custom_hours_adrift=2.5,
            )
            sar_triggered = True
            incident_id = incident.incident_id
            drift_datum = {
                "lat": incident.datum_lat,
                "lon": incident.datum_lon,
                "drift_distance_nm": incident.drift_distance_nm,
                "search_radius_nm": incident.search_radius_nm,
            }
        except Exception as e:
            logger.warning(f"Failed to declare SAR incident: {e}")
    elif res.status == ProbeStatus.ROAMING_BORDER_CROSS:
        ping = TelemetryPing.create(
            entity_id=f"canoe-{res.phone_number}",
            lat=6.1150,
            lon=1.2350,
            speed_mps=1.2,
            heading_deg=85.0,
            timestamp_ns=time.time_ns(),
        )
        _engine.ingest_telemetry(ping)

    return {
        "probe_result": res.model_dump(),
        "sar_triggered": sar_triggered,
        "incident_id": incident_id,
        "drift_datum": drift_datum,
    }


@app.post("/v1/probe/simulate-capsize", tags=["Self-Hosted Radio Probing & Watchdog"])
def simulate_capsize_event(req: SilentProbeRequest) -> Dict[str, Any]:
    """Simulate canoe capsize / phone water immersion and execute automated probe."""
    probe_engine.simulate_water_immersion(req.phone_number)
    return execute_silent_probe(req)


@app.post("/v1/probe/simulate-roaming", tags=["Self-Hosted Radio Probing & Watchdog"])
def simulate_roaming_event(req: SilentProbeRequest) -> Dict[str, Any]:
    """Simulate canoe drifting into Togolese waters and execute automated probe."""
    probe_engine.simulate_togo_roaming(req.phone_number)
    return execute_silent_probe(req)


@app.post("/v1/probe/reset", tags=["Self-Hosted Radio Probing & Watchdog"])
def reset_probe_event(req: SilentProbeRequest) -> Dict[str, Any]:
    """Reset subscriber state to normal active baseline."""
    probe_engine.reset_subscriber_state(req.phone_number)
    return execute_silent_probe(req)



# -------------------------------------------------------------------------
# OpenCellID Global Cellular Registry & Cell Tower Geolocation Endpoints
# -------------------------------------------------------------------------

class OpenCellIDSyncRequest(BaseModel):
    api_token: str
    mcc: int = 620


@app.get("/v1/opencellid/lookup", tags=["OpenCellID Global Cellular Registry"])
def lookup_opencellid_tower(
    cell_id: int = Query(...),
    lac: Optional[int] = Query(None),
    mnc: Optional[int] = Query(None),
) -> Dict[str, Any]:
    """Resolve a raw Cell ID / LAC from carrier signaling into exact GPS coordinates."""
    tower = opencellid_mgr.lookup(cell_id=cell_id, lac=lac, mnc=mnc)
    if not tower:
        raise HTTPException(status_code=404, detail=f"Cell ID {cell_id} not found in OpenCellID Ghana database.")
    return tower


@app.get("/v1/opencellid/stats", tags=["OpenCellID Global Cellular Registry"])
def get_opencellid_stats() -> Dict[str, Any]:
    """Return total indexed towers across Ghana (MCC 620)."""
    return opencellid_mgr.get_stats()


@app.post("/v1/opencellid/sync", tags=["OpenCellID Global Cellular Registry"])
def sync_opencellid_dataset(req: OpenCellIDSyncRequest) -> Dict[str, Any]:
    """Download and ingest the latest 620.csv.gz extract directly from OpenCellID."""
    try:
        return opencellid_mgr.download_and_ingest(api_token=req.api_token, mcc=req.mcc)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to sync OpenCellID: {e}")



# -------------------------------------------------------------------------
# -------------------------------------------------------------------------
# Sovereign SDR Airborne/Coastal Cellular SAR Intercept Endpoints
# -------------------------------------------------------------------------

@app.get("/v1/sdr/stations", tags=["SDR Cellular SAR Intercept"])
def get_sdr_stations() -> List[Dict[str, Any]]:
    """Return active coastal radar masts, drone UAVs, and Navy patrol craft equipped with SDR payloads."""
    return sdr_payload.get_stations()


@app.post("/v1/sdr/scan", tags=["SDR Cellular SAR Intercept"])
def scan_sdr_pocket_phone(req: SDRScanRequest) -> Dict[str, Any]:
    """
    Execute zero-interaction external cellular radio scan for a victim's phone.
    Detects physical RACH RF bursts, Time-of-Flight (ToF), and Angle-of-Arrival (AoA)
    without requiring any user link click, app, or interaction.
    Immediately updates the tactical radar and vectors nearby commercial AIS vessels.
    """
    clean_phone = normalize_ghana_phone(req.phone_number)
    
    # Check if subscriber exists in durable store for canoe name / landing beach
    sub = durable_store.get_subscriber(clean_phone)
    approx_pos = None
    if sub and sub.last_lat and sub.last_lon:
        approx_pos = (sub.last_lat, sub.last_lon)
    elif clean_phone.endswith("318"): # 0243875318 default realistic coastal sector
        approx_pos = (5.8620, 1.0340)

    fix = sdr_payload.execute_scan(
        phone_number=clean_phone,
        station_id=req.search_station_id,
        band=req.band,
        known_approx_pos=approx_pos,
    )
    
    canoe_name = sub.canoe_name if sub else "Agbe Yeye"
    landing_beach = sub.landing_beach if sub else "Keta Beach"
    
    # 1. Update Durable Store with verified external RF fix
    now = time.time()
    rec = SubscriberRecord(
        phone_number=clean_phone,
        owner_name=sub.owner_name if sub else canoe_name,
        canoe_name=canoe_name,
        landing_beach=landing_beach,
        language="Ewe" if "Keta" in landing_beach or "Vodza" in landing_beach else "Fante",
        status="SDR_RF_ACQUIRED",
        registered_at=sub.registered_at if sub else now,
        last_seen_at=now,
        last_tower_id=fix.detected_via_station,
        last_timing_advance=int(fix.estimated_distance_m / 554.0),
        last_lat=fix.lat,
        last_lon=fix.lon,
        distance_offshore_km=round(abs(fix.lat - 5.92) * 111.0, 2),
    )
    durable_store.save_subscriber(rec)

    # 2. Ingest into GeoMesh Spatial Engine
    ping = TelemetryPing.create(
        entity_id=f"canoe-{clean_phone}",
        lat=fix.lat,
        lon=fix.lon,
        speed_mps=1.2, # slight Guinea current drift
        heading_deg=85.0,
        timestamp_ns=time.time_ns(),
    )
    _engine.ingest_telemetry(ping)

    # 3. Find closest commercial vessel on live AIS for SAR rendezvous
    closest_vessel = None
    try:
        nearby_entities = _engine.query_radius(Point(fix.lat, fix.lon), radius_m=60000.0)
        for ent_id, pt, d_m in nearby_entities:
            if ent_id.startswith("mmsi-"):
                d_nm = d_m / 1852.0
                bearing_to_vessel = initial_bearing(Point(fix.lat, fix.lon), pt)
                state = _engine.get_kinematic_state(ent_id)
                speed_kt = round(state.speed_mps * 1.94384, 1) if state else 0.0
                closest_vessel = {
                    "mmsi": ent_id.replace("mmsi-", ""),
                    "distance_nm": round(d_nm, 2),
                    "bearing_deg": round(bearing_to_vessel, 1),
                    "lat": pt.lat,
                    "lon": pt.lon,
                    "speed_knots": speed_kt,
                }
                break
    except Exception:
        pass

    return {
        "status": "RF_FIX_ESTABLISHED",
        "target": fix.model_dump(),
        "canoe_name": canoe_name,
        "landing_beach": landing_beach,
        "closest_commercial_ship": closest_vessel,
        "external_protocol": "3GPP TS 04.08 RACH Uplink Time-of-Flight",
        "user_interaction_required": False,
        "handset_screen_unlocked": False,
        "weblink_clicked": False,
    }


@app.post("/v1/sdr/triangulate", tags=["SDR Cellular SAR Intercept"])
def triangulate_sdr_target(req: SDRScanRequest) -> Dict[str, Any]:
    """Execute multi-station bearing and timing advance intersection."""
    clean_phone = normalize_ghana_phone(req.phone_number)
    return sdr_payload.triangulate(clean_phone)




# Mobile Beach Launch & Handset GPS Departure Tracker (/launch)
# -------------------------------------------------------------------------

class LiveGPSReportRequest(BaseModel):
    phone_number: str
    canoe_name: str
    landing_beach: str = "Keta Beach"
    lat: float
    lon: float
    accuracy_m: float = 5.0
    speed_knots: float = 0.0
    heading_deg: float = 0.0
    is_sos: bool = False


@app.post("/v1/telecom/live-gps", tags=["Mobile Departure & Live Handset GPS"])
def ingest_live_handset_gps(req: LiveGPSReportRequest) -> Dict[str, Any]:
    """
    Ingest live GPS coordinates streaming from a fisherman's mobile browser.
    Creates or updates the canoe in the spatial mesh and durable database.
    If SOS is triggered, immediately initiates Guinea Current SAR drift calculation.
    """
    clean_phone = normalize_ghana_phone(req.phone_number)
    speed_mps = req.speed_knots * 0.514444
    now = time.time()

    # 1. Update Durable Store
    sub = durable_store.get_subscriber(clean_phone)
    status_str = "SOS_DISTRESS" if req.is_sos else ("AT_SEA" if req.speed_knots > 0.5 else "SAFE_ON_SHORE")
    rec = SubscriberRecord(
        phone_number=clean_phone,
        owner_name=sub.owner_name if sub else req.canoe_name,
        canoe_name=req.canoe_name,
        landing_beach=req.landing_beach,
        language="Ewe" if "Keta" in req.landing_beach or "Vodza" in req.landing_beach else "Fante",
        status=status_str,
        registered_at=sub.registered_at if sub else now,
        last_seen_at=now,
        last_tower_id="HANDSET_INTERNAL_GPS",
        last_timing_advance=0,
        last_lat=req.lat,
        last_lon=req.lon,
        distance_offshore_km=round(abs(req.lat - 5.92) * 111.0, 2),
    )
    durable_store.save_subscriber(rec)

    # 2. Ingest into GeoMesh Spatial Engine
    ping = TelemetryPing.create(
        entity_id=f"canoe-{clean_phone}",
        lat=req.lat,
        lon=req.lon,
        speed_mps=speed_mps,
        heading_deg=req.heading_deg,
        timestamp_ns=time.time_ns(),
    )
    _engine.ingest_telemetry(ping)

    # 3. Handle SOS Distress trigger
    sar_triggered = False
    incident_id = None
    drift_datum = None

    if req.is_sos:
        try:
            incident = sar_watchdog.declare_incident(
                phone_number=clean_phone,
                reason="LIVE_HANDSET_GPS_SOS_EMERGENCY",
                custom_hours_adrift=1.0,
            )
            sar_triggered = True
            incident_id = incident.incident_id
            drift_datum = {
                "lat": incident.datum_lat,
                "lon": incident.datum_lon,
                "drift_distance_nm": incident.drift_distance_nm,
                "search_radius_nm": incident.search_radius_nm,
            }
        except Exception as e:
            logger.warning(f"Failed to declare SAR from live GPS SOS: {e}")

    return {
        "status": "INGESTED",
        "entity_id": f"canoe-{clean_phone}",
        "lat": req.lat,
        "lon": req.lon,
        "sar_triggered": sar_triggered,
        "incident_id": incident_id,
        "drift_datum": drift_datum,
        "server_time_utc": time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(now)),
    }


@app.get("/launch", response_class=HTMLResponse, tags=["Mobile Departure & Live Handset GPS"])
def get_mobile_launch_page() -> str:
    """Serve lightweight high-contrast mobile departure and GPS tracker page."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>GH-MARSAR: Beach Departure GPS Tracker</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #030712;
      color: #f8fafc;
      font-family: 'Inter', sans-serif;
      padding: 16px;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }
    .header {
      background: rgba(15,23,42,0.85);
      border: 1px solid rgba(56,189,248,0.25);
      border-radius: 8px;
      padding: 14px;
      margin-bottom: 14px;
      text-align: center;
    }
    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 4px;
      font-size: 0.68rem;
      font-weight: 700;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      background: rgba(16,185,129,0.15);
      color: #10b981;
      border: 1px solid #10b981;
      margin-bottom: 6px;
    }
    .title { font-size: 1.1rem; font-weight: 800; color: #38bdf8; }
    .subtitle { font-size: 0.72rem; color: #94a3b8; margin-top: 4px; }
    .card {
      background: rgba(15,23,42,0.75);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 8px;
      padding: 14px;
      margin-bottom: 12px;
    }
    label { font-size: 0.72rem; font-weight: 700; color: #94a3b8; display: block; margin-bottom: 4px; }
    input, select {
      width: 100%;
      background: rgba(3,7,18,0.9);
      border: 1px solid rgba(255,255,255,0.18);
      border-radius: 6px;
      color: #fff;
      padding: 10px 12px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.85rem;
      margin-bottom: 10px;
    }
    .btn {
      width: 100%;
      padding: 14px;
      border: none;
      border-radius: 8px;
      font-weight: 800;
      font-size: 0.95rem;
      letter-spacing: 0.04em;
      cursor: pointer;
      display: flex;
      justify-content: center;
      align-items: center;
      gap: 8px;
      margin-bottom: 10px;
    }
    .btn-green { background: #10b981; color: #022c22; }
    .btn-green:active { background: #059669; }
    .btn-red { background: #ef4444; color: #fff; }
    .btn-red:active { background: #dc2626; }
    .status-box {
      background: #020617;
      border: 1px solid rgba(56,189,248,0.3);
      border-radius: 6px;
      padding: 10px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.7rem;
      color: #38bdf8;
      line-height: 1.5;
    }
    .footer { font-size: 0.65rem; color: #64748b; text-align: center; margin-top: 10px; }
  </style>
</head>
<body>
  <div>
    <div class="header">
      <span class="badge">GHANA MARITIME AUTHORITY</span>
      <div class="title">🇬🇭 ARTISANAL BEACH LAUNCH</div>
      <div class="subtitle">Live Handset GPS Departure Tracker (Zero-Install)</div>
    </div>

    <div class="card">
      <label>CANOE NAME / REGISTRATION</label>
      <input type="text" id="canoeName" value="Agbe Yeye">

      <label>TELEPHONE NUMBER (MSISDN)</label>
      <input type="tel" id="phoneNumber" value="0243875318">

      <label>DEPARTURE LANDING BEACH</label>
      <select id="landingBeach">
        <option value="Keta Beach" selected>Keta Beach (Volta)</option>
        <option value="Vodza">Vodza Fish Market (Volta)</option>
        <option value="Ada Foah">Ada Foah Volta Estuary (Greater Accra)</option>
        <option value="Tema Fishing Harbor">Tema Fishing Harbor (Accra)</option>
        <option value="Jamestown Beach">Jamestown Accra Beach</option>
        <option value="Elmina">Elmina Fishing Harbor (Central)</option>
        <option value="Takoradi Port">Takoradi Fishery Beach (Western)</option>
        <option value="Axim">Axim Landing Beach (Western)</option>
      </select>

      <button id="startBtn" class="btn btn-green" onclick="toggleTracking()">
        <span>🟢 DEPART BEACH (START TRACKING)</span>
      </button>

      <button id="sosBtn" class="btn btn-red" onclick="triggerSOS()">
        <span>🚨 EMERGENCY SOS DISTRESS</span>
      </button>
    </div>

    <div class="card">
      <label>LIVE TRANSMISSION TELEMETRY</label>
      <div class="status-box" id="statusDisplay">
        STATUS: READY AT BEACH<br>
        GPS: WAITING FOR DEPARTURE TAP...<br>
        PINGS SENT: 0<br>
        RADAR FEED: STANDBY
      </div>
    </div>
  </div>

  <div class="footer">
    Ghana Maritime Authority (Act 630) • National Maritime Rescue Coordination Centre (Accra MRCC)
  </div>

  <script>
    let watchId = null;
    let isTracking = false;
    let pingCount = 0;
    let currentLat = 5.8250;
    let currentLon = 0.9150;
    let currentSpeed = 0.0;
    let currentHeading = 0.0;
    let timerId = null;

    function toggleTracking() {
      const btn = document.getElementById('startBtn');
      if (!isTracking) {
        isTracking = true;
        btn.innerHTML = '<span>🔴 STOP TRACKING (MOORED SAFE)</span>';
        btn.style.background = '#f59e0b';
        btn.style.color = '#000';
        startGPS();
      } else {
        isTracking = false;
        btn.innerHTML = '<span>🟢 DEPART BEACH (START TRACKING)</span>';
        btn.style.background = '#10b981';
        btn.style.color = '#022c22';
        stopGPS();
      }
    }

    function startGPS() {
      if ('geolocation' in navigator) {
        watchId = navigator.geolocation.watchPosition(
          pos => {
            currentLat = pos.coords.latitude;
            currentLon = pos.coords.longitude;
            currentSpeed = pos.coords.speed ? (pos.coords.speed * 1.94384) : 2.5;
            currentHeading = pos.coords.heading || 160.0;
            sendPing(false, pos.coords.accuracy || 4.0);
          },
          err => {
            console.warn('Geolocation fallback:', err);
            // Fallback for simulation / testing
            sendPing(false, 5.0);
          },
          { enableHighAccuracy: true, maximumAge: 2000, timeout: 5000 }
        );
      } else {
        sendPing(false, 5.0);
      }

      // Interval fallback ensuring regular 4s transmission
      timerId = setInterval(() => {
        if (isTracking) sendPing(false, 5.0);
      }, 4000);
    }

    function stopGPS() {
      if (watchId) navigator.geolocation.clearWatch(watchId);
      if (timerId) clearInterval(timerId);
      document.getElementById('statusDisplay').innerHTML = `
        STATUS: MOORED SAFE ON BEACH<br>
        TRACKING: STOPPED<br>
        TOTAL PINGS TRANSMITTED: ${pingCount}
      `;
    }

    async function sendPing(isSOS, accuracy) {
      pingCount++;
      const canoe = document.getElementById('canoeName').value.trim() || 'Agbe Yeye';
      const phone = document.getElementById('phoneNumber').value.trim() || '0243875318';
      const beach = document.getElementById('landingBeach').value;

      const payload = {
        phone_number: phone,
        canoe_name: canoe,
        landing_beach: beach,
        lat: currentLat,
        lon: currentLon,
        accuracy_m: accuracy || 5.0,
        speed_knots: currentSpeed,
        heading_deg: currentHeading,
        is_sos: isSOS
      };

      try {
        const resp = await fetch('/v1/telecom/live-gps', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload)
        });
        if (resp.ok) {
          const res = await resp.json();
          document.getElementById('statusDisplay').innerHTML = `
            STATUS: 🟢 BROADCASTING LIVE TO GMA RADAR<br>
            CANOE: ${canoe} (${phone})<br>
            GPS FIX: [${currentLat.toFixed(4)}, ${currentLon.toFixed(4)}] ±${accuracy.toFixed(1)}m<br>
            SPEED: ${currentSpeed.toFixed(1)} knots &bull; HEADING: ${currentHeading.toFixed(0)}&deg;<br>
            PINGS SENT: ${pingCount}<br>
            SERVER TIME: ${res.server_time_utc}
          `;
        }
      } catch (e) {
        document.getElementById('statusDisplay').innerHTML += `<br><span style="color:#f87171;">OFFLINE: Fix cached. Will flush on signal.</span>`;
      }
    }

    function triggerSOS() {
      sendPing(true, 3.0);
      document.getElementById('statusDisplay').innerHTML = `
        <span style="color:#ef4444;font-weight:700;">🚨 EMERGENCY SOS DISPATCHED!</span><br>
        Ghana Maritime Authority MRCC Alerted!<br>
        Guinea Current Datum Calculated for Rescue Boats!
      `;
    }
  </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
def get_map_dashboard() -> str:
    """Serve GH-MARSAR National Coastal Operations Command Center."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>GH-MARSAR: Ghana National Maritime Rescue & Artisanal Safety Network (GeoMesh-Stream)</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    :root {
      --bg-space: #030712;
      --panel-bg: rgba(10, 16, 31, 0.88);
      --panel-border: rgba(255, 255, 255, 0.08);
      --panel-border-focus: rgba(56, 189, 248, 0.4);
      --accent-cyan: #38bdf8;
      --accent-emerald: #10b981;
      --accent-amber: #f59e0b;
      --accent-rose: #ef4444;
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
      --text-faint: #64748b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; padding: 0;
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg-space);
      color: var(--text-main);
      overflow: hidden;
      height: 100vh;
      width: 100vw;
    }
    
    /* Fullscreen Edge-to-Edge Nautical Radar Canvas */
    #map {
      position: absolute;
      top: 0; left: 0; right: 0; bottom: 0;
      width: 100vw;
      height: 100vh;
      z-index: 1;
      background: #020617;
    }

    /* Floating Glass HUD Panels */
    .glass-panel {
      background: var(--panel-bg);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      border: 1px solid var(--panel-border);
      border-radius: 10px;
      box-shadow: 0 16px 36px 0 rgba(0, 0, 0, 0.65);
      z-index: 1000;
      pointer-events: auto;
    }

    /* Top National Command Header */
    #topbar {
      position: absolute;
      top: 14px;
      left: 16px;
      right: 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 18px;
    }
    .brand-section {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .gh-flag-badge {
      display: inline-flex;
      height: 18px;
      width: 28px;
      border-radius: 3px;
      overflow: hidden;
      box-shadow: 0 0 8px rgba(0,0,0,0.4);
      border: 1px solid rgba(255,255,255,0.15);
    }
    .gh-red { flex: 1; background: #ce1126; }
    .gh-yellow { flex: 1; background: #fcd116; position: relative; }
    .gh-green { flex: 1; background: #006b3f; }
    
    .brand-title {
      font-size: 0.95rem;
      font-weight: 800;
      letter-spacing: 0.04em;
      color: #fff;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .brand-badge {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.65rem;
      background: rgba(56, 189, 248, 0.15);
      color: var(--accent-cyan);
      border: 1px solid rgba(56, 189, 248, 0.3);
      padding: 2px 6px;
      border-radius: 4px;
      font-weight: 600;
      letter-spacing: 0.05em;
    }
    .live-dot {
      display: inline-block;
      width: 7px;
      height: 7px;
      background: #10b981;
      border-radius: 50%;
      box-shadow: 0 0 8px #10b981;
      animation: pulse-dot 2s infinite;
    }
    @keyframes pulse-dot {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }

    .telemetry-center {
      display: flex;
      align-items: center;
      gap: 16px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.75rem;
      color: var(--text-muted);
    }
    .telemetry-item {
      display: flex;
      align-items: center;
      gap: 6px;
      background: rgba(15, 23, 42, 0.6);
      padding: 4px 10px;
      border-radius: 6px;
      border: 1px solid rgba(255,255,255,0.05);
    }
    .telemetry-val {
      color: #f8fafc;
      font-weight: 700;
    }

    .stat-badge {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.74rem;
      background: rgba(15, 23, 42, 0.7);
      border: 1px solid rgba(255,255,255,0.08);
      padding: 5px 12px;
      border-radius: 6px;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .stat-val { font-weight: 800; color: var(--accent-cyan); }
    .stat-sos { color: var(--accent-rose); font-weight: 800; }

    /* Sector Navigation Floating Bar */
    #navsectors {
      position: absolute;
      top: 72px;
      left: 16px;
      display: flex;
      gap: 6px;
      z-index: 1000;
      padding: 4px;
      background: rgba(10, 16, 31, 0.8);
      backdrop-filter: blur(12px);
      border: 1px solid var(--panel-border);
      border-radius: 8px;
    }
    .nav-btn {
      background: transparent;
      color: var(--text-muted);
      border: none;
      padding: 6px 12px;
      border-radius: 5px;
      cursor: pointer;
      font-size: 0.72rem;
      font-weight: 600;
      transition: all 0.2s;
    }
    .nav-btn:hover {
      background: rgba(255,255,255,0.05);
      color: #fff;
    }
    .nav-btn.active {
      background: #0284c7;
      color: #fff;
      box-shadow: 0 0 12px rgba(2, 132, 199, 0.5);
    }

    /* Left Tactical Control Deck (Floating Collapsible) */
    #sidebar {
      position: absolute;
      top: 120px;
      left: 16px;
      bottom: 60px;
      width: 420px;
      display: flex;
      flex-direction: column;
      z-index: 1000;
      overflow: hidden;
      transition: transform 0.3s cubic-bezier(0.16, 1, 0.3, 1);
    }
    .deck-header {
      padding: 12px 14px 6px 14px;
      border-bottom: 1px solid var(--panel-border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .deck-tabs {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 4px;
      padding: 8px 12px;
      background: rgba(6, 10, 20, 0.6);
      border-bottom: 1px solid var(--panel-border);
    }
    .tab-btn {
      background: transparent;
      border: 1px solid transparent;
      color: var(--text-muted);
      font-size: 0.68rem;
      font-weight: 700;
      padding: 6px 4px;
      border-radius: 5px;
      cursor: pointer;
      text-align: center;
      transition: all 0.15s;
      white-space: nowrap;
    }
    .tab-btn.active {
      background: rgba(56, 189, 248, 0.15);
      color: var(--accent-cyan);
      border-color: rgba(56, 189, 248, 0.3);
    }
    .deck-content {
      flex: 1;
      overflow-y: auto;
      padding: 14px;
    }

    .card {
      background: rgba(15, 23, 42, 0.65);
      border: 1px solid var(--panel-border);
      border-radius: 8px;
      padding: 12px;
      margin-bottom: 12px;
    }
    .card-emergency {
      border-color: rgba(239, 68, 68, 0.4);
      background: rgba(36, 12, 20, 0.6);
      box-shadow: 0 0 16px rgba(239,68,68,0.15);
    }
    .card h3 {
      margin-top: 0;
      font-size: 0.76rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      margin-bottom: 8px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .card-emergency h3 { color: #f87171; }

    /* Tactical Inputs & Buttons */
    input[type="text"], select {
      width: 100%;
      background: #060b16;
      border: 1px solid #1e293b;
      color: #f8fafc;
      padding: 8px 10px;
      border-radius: 6px;
      font-size: 0.74rem;
      font-family: 'Inter', sans-serif;
      transition: border-color 0.2s, box-shadow 0.2s;
    }
    input[type="text"]:focus, select:focus {
      outline: none;
      border-color: var(--accent-cyan);
      box-shadow: 0 0 8px rgba(56, 189, 248, 0.3);
    }
    
    .btn {
      background: #0284c7;
      color: #ffffff;
      border: none;
      padding: 8px 12px;
      border-radius: 6px;
      cursor: pointer;
      font-weight: 600;
      font-size: 0.76rem;
      transition: all 0.2s;
      display: inline-flex;
      justify-content: center;
      align-items: center;
      gap: 6px;
    }
    .btn:hover {
      background: #0369a1;
      box-shadow: 0 0 12px rgba(2, 132, 199, 0.4);
    }
    .btn-danger {
      background: #dc2626;
    }
    .btn-danger:hover {
      background: #b91c1c;
      box-shadow: 0 0 12px rgba(220, 38, 38, 0.5);
    }
    .btn-emerald {
      background: #059669;
    }
    .btn-emerald:hover {
      background: #047857;
      box-shadow: 0 0 12px rgba(5, 150, 105, 0.4);
    }
    .btn-sm {
      padding: 4px 8px;
      font-size: 0.68rem;
    }

    /* Bottom Floating Telemetry Log */
    #bottomDock {
      position: absolute;
      bottom: 12px;
      left: 16px;
      right: 16px;
      z-index: 1000;
      transition: max-height 0.3s;
      max-height: 140px;
      display: flex;
      flex-direction: column;
    }
    .dock-bar {
      padding: 6px 14px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 0.72rem;
      font-family: 'JetBrains Mono', monospace;
      color: var(--text-muted);
      cursor: pointer;
      border-bottom: 1px solid var(--panel-border);
    }
    .event-log {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.72rem;
      height: 100px;
      overflow-y: auto;
      padding: 8px 14px;
      background: rgba(3, 7, 18, 0.95);
      line-height: 1.6;
    }
    .event-cpa { color: #f87171; font-weight: 700; }
    .event-sar { color: #fbbf24; font-weight: 700; }
    .event-info { color: #38bdf8; }
    .event-enter { color: #34d399; }

    /* Tactical Leaflet Dark Glass Popups & Controls */
    .leaflet-popup-content-wrapper {
      background: rgba(10, 16, 31, 0.95) !important;
      color: var(--text-main) !important;
      border: 1px solid rgba(56, 189, 248, 0.35) !important;
      border-radius: 8px !important;
      box-shadow: 0 12px 32px rgba(0, 0, 0, 0.8), 0 0 16px rgba(56, 189, 248, 0.15) !important;
      backdrop-filter: blur(12px) !important;
      -webkit-backdrop-filter: blur(12px) !important;
      padding: 4px !important;
    }
    .leaflet-popup-content {
      font-family: 'Inter', -apple-system, sans-serif !important;
      font-size: 0.75rem !important;
      line-height: 1.5 !important;
      margin: 8px 12px !important;
      color: #cbd5e1 !important;
    }
    .leaflet-popup-content b, .leaflet-popup-content strong {
      color: #f8fafc !important;
    }
    .leaflet-popup-tip {
      background: rgba(10, 16, 31, 0.95) !important;
      border: 1px solid rgba(56, 189, 248, 0.35) !important;
      box-shadow: none !important;
    }
    .leaflet-container a.leaflet-popup-close-button {
      color: #94a3b8 !important;
      padding: 6px 24px 0 0 !important;
    }
    .leaflet-container a.leaflet-popup-close-button:hover {
      color: #38bdf8 !important;
    }

    /* Tactical Leaflet Overrides */
    .leaflet-bar a {
      background-color: var(--panel-bg) !important;
      border-bottom: 1px solid var(--panel-border) !important;
      color: #e2e8f0 !important;
    }
    .leaflet-bar a:hover {
      background-color: #1e293b !important;
      color: #38bdf8 !important;
    }
    .leaflet-control-zoom {
      border: 1px solid var(--panel-border) !important;
      border-radius: 8px !important;
      overflow: hidden;
      box-shadow: 0 8px 24px rgba(0,0,0,0.5) !important;
    }
  </style>
</head>
<body>
  <!-- Full-Screen Tactical Canvas -->
  <div id="map"></div>

  <!-- Top Floating National Command Header -->
  <div id="topbar" class="glass-panel">
    <div class="brand-section">
      <span class="gh-flag-badge">
        <span class="gh-red"></span><span class="gh-yellow"></span><span class="gh-green"></span>
      </span>
      <div>
        <div class="brand-title">
          <span>GH-MARSAR</span>
          <span class="brand-badge">VTS C4ISR RADAR</span>
          <span class="live-dot" title="Network Connected"></span>
        </div>
        <div style="font-size:0.65rem;color:var(--text-faint);letter-spacing:0.02em;">
          Ghana Maritime Authority &bull; National Rescue &amp; Artisanal GSM Telemetry Mesh
        </div>
      </div>
    </div>

    <!-- Real-time Tactical Gauges -->
    <div class="telemetry-center">
      <div class="telemetry-item">
        <span style="color:var(--text-faint);">CLOCK:</span>
        <span id="liveUTCClock" class="telemetry-val">00:00:00 UTC</span>
      </div>
      <div class="telemetry-item">
        <span style="color:var(--text-faint);">GUINEA CURRENT:</span>
        <span class="telemetry-val" style="color:var(--accent-amber);">0.85 m/s &bull; 090&deg; E</span>
      </div>
      <div class="telemetry-item">
        <span style="color:var(--text-faint);">BTS MESH:</span>
        <span class="telemetry-val" style="color:var(--accent-emerald);">29 Towers Active</span>
      </div>
      <div class="telemetry-item">
        <span style="color:var(--text-faint);">LIVE AIS:</span>
        <span id="liveAISBadge" class="telemetry-val" style="color:#10b981;">CONNECTING...</span>
      </div>
    </div>

    <!-- Fleet Status Metrics Badges -->
    <div style="display:flex;gap:6px;">
      <div class="stat-badge">ENROLLED: <span id="statEnrolled" class="stat-val">0</span></div>
      <div class="stat-badge">AT SEA: <span id="statAtSea" class="stat-val" style="color:#38bdf8;">0</span></div>
      <div class="stat-badge">ON SHORE: <span id="statOnShore" class="stat-val" style="color:#10b981;">0</span></div>
      <div class="stat-badge" style="border-color:rgba(245,158,11,0.3);">LIVE SHIPS: <span id="statShips" class="stat-val" style="color:#f59e0b;">0</span></div>
      <div class="stat-badge" style="border-color:rgba(239,68,68,0.4);background:rgba(239,68,68,0.08);">
        🚨 OVERDUE / SOS: <span id="statSOS" class="stat-sos">0</span>
      </div>
    </div>
  </div>

  <!-- Sector Navigation Floating Pills -->
  <div id="navsectors">
    <button class="nav-btn active" onclick="zoomSector('ALL', this)">🇬🇭 All Coast (550km)</button>
    <button class="nav-btn" onclick="zoomSector('WESTERN', this)">Western (Axim / Takoradi)</button>
    <button class="nav-btn" onclick="zoomSector('CENTRAL', this)">Central (Elmina / Winneba)</button>
    <button class="nav-btn" onclick="zoomSector('ACCRA', this)">Greater Accra (Tema / Ada)</button>
    <button class="nav-btn" onclick="zoomSector('VOLTA', this)">Volta (Keta / Aflao)</button>
  </div>

  <!-- Left Tactical Control Deck (Floating Collapsible Glass Deck) -->
  <div id="sidebar" class="glass-panel">
    <div class="deck-header">
      <div style="font-weight:700;font-size:0.8rem;letter-spacing:0.04em;display:flex;align-items:center;gap:6px;">
        <span style="color:var(--accent-cyan);">TACTICAL OPERATIONS DECK</span>
      </div>
      <span id="rosterCount" style="font-family:'JetBrains Mono',monospace;font-size:0.7rem;color:var(--accent-cyan);">0 boats</span>
    </div>

    <!-- Navigation Tabs for Tactical Deck -->
    <div class="deck-tabs">
      <button class="tab-btn active" onclick="switchDeckTab('LOCATE', this)">🛰️ LOCATOR</button>
      <button class="tab-btn" onclick="switchDeckTab('PROBE', this)">⚡ ZERO-COST PROBE</button>
      <button class="tab-btn" onclick="switchDeckTab('SDR', this)">📡 SDR PHONE FINDER</button>
      <button class="tab-btn" onclick="switchDeckTab('SAR', this)">🚨 SAR HOTLINE</button>
      <button class="tab-btn" onclick="switchDeckTab('ROSTER', this)">🛶 FLEET</button>
      <button class="tab-btn" onclick="switchDeckTab('OPS', this)">⚡ OPS &amp; AIS</button>
    </div>

    <div class="deck-content">
      <!-- TAB 1: REAL-TIME CANOE LOCATOR (KETA-VODZA RESOLVER) -->
      <div id="tabContentLocate">
        <div class="card" style="border-color:rgba(56,189,248,0.3);background:rgba(8,20,38,0.7);">
          <h3 style="color:var(--accent-cyan);">
            <span>🛰️ Real-Time GSM Locator</span>
            <span style="font-size:0.62rem;background:#0284c7;color:#fff;padding:2px 5px;border-radius:3px;">ZERO-APP</span>
          </h3>
          <p style="font-size:0.71rem;color:var(--text-muted);line-height:1.4;margin:4px 0 8px 0;">
            Pinpoint feature phones instantly via silent radio paging. Verifies if a canoe is missing at sea or safely landed at another beach (e.g. <strong>Keta &rarr; Vodza</strong>).
          </p>
          <div style="display:flex;gap:6px;margin-bottom:8px;">
            <input id="locateInput" type="text" placeholder="Phone or Canoe Name (e.g. Dzidula)" list="canoeSuggestions" />
            <datalist id="canoeSuggestions"></datalist>
            <button class="btn btn-sm" onclick="locateCanoeRealtime()" style="white-space:nowrap;">🔍 LOCATE</button>
          </div>
          
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:8px;">
            <button class="btn btn-sm btn-emerald" onclick="simulateKetaToVodza()">🚤 Relocate to Vodza (Sell Fish)</button>
            <button class="btn btn-sm" style="background:#334155;" onclick="simulateReturnToKeta()">🏖️ Return to Keta Beach</button>
          </div>

          <div id="locateResultBox" style="font-size:0.72rem;line-height:1.4;background:#050914;border:1px solid #1e293b;border-radius:6px;padding:8px;display:none;"></div>
        </div>
      </div>

      <!-- TAB 2: SAR HOTLINE & OVERDUE REPORTING -->
      
      <!-- TAB: SELF-HOSTED SILENT PROBE & WATER IMMERSION WATCHDOG -->
      
      <!-- TAB: SOVEREIGN SDR CELLULAR SAR INTERCEPT (ZERO-TOUCH POCKET PHONE FINDER) -->
      <div id="tabContentSDR" style="display:none;">
        <div style="background:rgba(15,23,42,0.65);border:1px solid rgba(56,189,248,0.25);border-radius:6px;padding:12px;margin-bottom:10px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <strong style="color:var(--accent-cyan);font-size:0.75rem;">📡 EXTERNAL SDR PHONE FINDER</strong>
            <span class="badge" style="background:rgba(56,189,248,0.15);color:#38bdf8;border:1px solid #38bdf8;">NO LINK NEEDED</span>
          </div>
          <p style="font-size:0.70rem;color:var(--text-muted);line-height:1.35;margin-bottom:8px;">
            Locates basic feature phones & smartphones <em>in pockets at sea</em> without clicking links, apps, or data connection. Search drone or coastal mast captures <strong>3GPP RACH RF Uplink pulses</strong> via Time-of-Flight & Angle-of-Arrival.
          </p>

          <div style="display:flex;flex-direction:column;gap:6px;margin-bottom:8px;">
            <div style="display:flex;gap:6px;">
              <input id="sdrPhoneInput" type="text" value="0243875318" placeholder="Target Phone (e.g. 0243875318)" style="flex:1;" />
              <select id="sdrStationSelect" style="flex:1;background:#0f172a;border:1px solid #334155;color:#f8fafc;padding:4px 6px;border-radius:4px;font-size:0.72rem;">
                <option value="DRONE_KETA_SAR_1">🚁 Navy Drone UAV 01 (Lifeseeker)</option>
                <option value="STATION_KETA_MAST">🗼 Keta Cape St Paul SDR Mast</option>
                <option value="NAVY_P30_PATROL">⚓ Navy P30 Patrol Craft SDR</option>
              </select>
            </div>
            
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;">
              <button class="btn btn-sm btn-emerald" onclick="executeSDRScan()">🚀 SCAN POCKET PHONE (RF)</button>
              <button class="btn btn-sm" style="background:#0284c7;color:#fff;" onclick="executeSDRTriangulate()">📐 TRIANGULATE FIX</button>
            </div>
          </div>

          <div id="sdrResultCard" style="display:none;background:#050914;border:1px solid rgba(56,189,248,0.3);border-radius:6px;padding:8px;font-size:0.72rem;line-height:1.4;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
              <span id="sdrBadge" class="badge" style="background:rgba(16,185,129,0.2);color:#10b981;border:1px solid #10b981;">RF FIX ACQUIRED</span>
              <span id="sdrTimeSpan" style="color:#64748b;font-family:monospace;"></span>
            </div>
            <div id="sdrTelemetryText" style="margin-bottom:4px;color:#e2e8f0;"></div>
            <div id="sdrShipAdvisory" style="background:rgba(245,158,11,0.1);border:1px solid rgba(245,158,11,0.3);border-radius:4px;padding:5px;margin-top:4px;color:#fbbf24;"></div>
          </div>
        </div>
      </div>

      <div id="tabContentProbe" style="display:none;">
        <div style="background:rgba(15,23,42,0.65);border:1px solid rgba(56,189,248,0.25);border-radius:6px;padding:12px;margin-bottom:10px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <strong style="color:var(--accent-cyan);font-size:0.75rem;">⚡ SELF-HOSTED SILENT RADIO PROBE</strong>
            <span class="badge" style="background:rgba(16,185,129,0.15);color:#10b981;border:1px solid #10b981;">ZERO PAID APIS</span>
          </div>
          <p style="font-size:0.68rem;color:var(--text-faint);margin:0 0 10px 0;line-height:1.3;">
            Sub-second SIP Ring-and-Drop signaling. Probes cell tower status without ringing user or billing money (Cost: GH₵ 0.00). If phone drowned, watchdog auto-calculates Guinea Current drift!
          </p>
          <div style="display:flex;gap:6px;margin-bottom:8px;">
            <input type="text" id="probePhoneInput" value="+233240003344" placeholder="Phone e.g. 0243875318" style="flex:1;background:rgba(3,7,18,0.8);border:1px solid rgba(255,255,255,0.15);border-radius:4px;color:#fff;padding:6px 10px;font-family:'JetBrains Mono',monospace;font-size:0.75rem;">
            <button class="btn btn-sm btn-cyan" onclick="runSilentProbe('NORMAL')">⚡ PROBE</button>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:8px;">
            <button class="btn btn-sm btn-red" onclick="runSilentProbe('CAPSIZE')">🌊 Water Capsize</button>
            <button class="btn btn-sm btn-amber" onclick="runSilentProbe('TOGO')">🇹🇬 Togo Roaming</button>
          </div>
          <button class="btn btn-sm" style="width:100%;background:rgba(255,255,255,0.06);color:var(--text-faint);" onclick="runSilentProbe('RESET')">🔄 Reset Normal State</button>
        </div>

        <div id="probeResultBox" style="display:none;background:rgba(3,7,18,0.85);border:1px solid rgba(255,255,255,0.12);border-radius:6px;padding:10px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <span id="probeStatusBadge" class="badge" style="font-size:0.7rem;">PROBE ACTIVE</span>
            <span style="font-family:'JetBrains Mono',monospace;font-size:0.68rem;color:#10b981;">Billing: GH₵ 0.00</span>
          </div>
          <div id="probeSummaryText" style="font-size:0.72rem;color:var(--text-light);margin-bottom:6px;"></div>
          <div style="font-size:0.65rem;color:var(--text-faint);margin-bottom:4px;font-weight:700;">SIP SIGNALING PROTOCOL TRACE (SUB-SECOND):</div>
          <div id="probeTraceBox" style="background:#020617;border:1px solid rgba(255,255,255,0.08);border-radius:4px;padding:6px;font-family:'JetBrains Mono',monospace;font-size:0.62rem;color:#38bdf8;max-height:120px;overflow-y:auto;white-space:pre-wrap;"></div>
        </div>
      </div>

      <div id="tabContentSAR" style="display:none;">
        <div class="card card-emergency">
          <h3>
            <span>🚨 Overdue Canoe / SAR Hotline</span>
            <span style="font-size:0.62rem;background:#dc2626;color:#fff;padding:2px 5px;border-radius:3px;">GMA &amp; NAVY</span>
          </h3>
          <p style="font-size:0.71rem;color:#cbd5e1;line-height:1.4;margin:4px 0 8px 0;">
            If a boat fails to return from night fishing, enter details below. Pulls last cell tower fix, extrapolates <strong>Guinea Current drift datum</strong>, and notifies rescue responders.
          </p>
          <div style="display:flex;gap:6px;margin-bottom:8px;">
            <input id="sarInput" type="text" placeholder="Phone or Canoe Name" />
            <button class="btn btn-danger btn-sm" onclick="reportMissingBoat()" style="white-space:nowrap;">DECLARE SAR</button>
          </div>
          <div id="activeSARBox" style="font-size:0.72rem;max-height:160px;overflow-y:auto;line-height:1.4;color:var(--text-muted);border-top:1px solid rgba(255,255,255,0.08);padding-top:6px;">
            No active missing boat incidents. Coast is clear.
          </div>
        </div>
      </div>

      <!-- TAB 3: ARTISANAL FLEET ROSTER -->
      <div id="tabContentRoster" style="display:none;">
        <div class="card">
          <h3>Enrolled Fleet Registry</h3>
          <div id="fleetTable" style="font-size:0.72rem;max-height:280px;overflow-y:auto;line-height:1.4;">
            Loading registered canoes...
          </div>
        </div>
      </div>

      <!-- TAB 4: OPERATIONS, REGISTRATION & SIMULATION -->
      <div id="tabContentOPS" style="display:none;">
        <!-- One-Time Canoe Enrollment -->
        <div class="card">
          <h3>
            <span>Enroll Canoe</span>
            <span style="font-size:0.62rem;background:#059669;color:#fff;padding:2px 5px;border-radius:3px;">ONE-TIME</span>
          </h3>
          <div style="display:flex;flex-direction:column;gap:6px;font-size:0.74rem;">
            <input id="regPhone" type="text" placeholder="Mobile Number (e.g. 0244123456)" />
            <input id="regCanoe" type="text" placeholder="Canoe Name (e.g. Nyame Nti)" />
            <div style="display:flex;gap:6px;">
              <select id="regBeach" style="flex:1;">
                <option value="Keta Beach">Volta: Keta Beach</option>
                <option value="Vodza">Volta: Vodza</option>
                <option value="Dzelukope">Volta: Dzelukope</option>
                <option value="Anloga">Volta: Anloga</option>
                <option value="Aflao">Volta: Aflao</option>
                <option value="Ada Foah">Accra: Ada Foah</option>
                <option value="Prampram">Accra: Prampram</option>
                <option value="Tema Manhean">Accra: Tema Manhean</option>
                <option value="Jamestown">Accra: Jamestown</option>
                <option value="Winneba">Central: Winneba</option>
                <option value="Elmina">Central: Elmina</option>
                <option value="Takoradi">Western: Takoradi</option>
                <option value="Axim">Western: Axim</option>
              </select>
              <select id="regLang" style="flex:1;">
                <option value="EWE">Ewe (Volta)</option>
                <option value="GA">Ga (Accra)</option>
                <option value="FANTE">Fante (Central)</option>
                <option value="DANGME">Dangme (Ada)</option>
                <option value="NZEMA">Nzema (Western)</option>
                <option value="ENGLISH">English</option>
              </select>
            </div>
            <button class="btn" onclick="enrollCanoe()" style="background:#4f46e5;">Register Canoe Once</button>
          </div>
        </div>

        <!-- Operational Triggers -->
        <div class="card">
          <h3>Operational Field Actions</h3>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;">
            <button class="btn btn-sm btn-danger" onclick="simulateKeypadSOS()">Trigger Keypad SOS (*920*44#)</button>
            <button class="btn btn-sm" style="background:#0891b2;" onclick="injectAISVessel()">Inject AIS Tanker (Tema VHF)</button>
            <button class="btn btn-sm btn-emerald" onclick="simulateSeawardTrip()">Simulate Seaward Launch</button>
            <button class="btn btn-sm" style="background:#334155;" onclick="clearRadar()">Clear Test Radar Marks</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- Bottom Floating Telemetry Stream Dock -->
  <div id="bottomDock" class="glass-panel">
    <div class="dock-bar" onclick="toggleTelemetryDock()">
      <span>▼ REAL-TIME TELEMETRY STREAM &bull; GULF OF GUINEA SURVEILLANCE</span>
      <span style="color:var(--accent-cyan);font-size:0.68rem;">LIVE LOG</span>
    </div>
    <div id="eventLog" class="event-log">National surveillance active across 550km coastline...</div>
  </div>

  <script>
    // Live UTC Clock updater
    function updateClock() {
      const now = new Date();
      const utc = now.toISOString().substring(11, 19) + ' UTC';
      document.getElementById('liveUTCClock').innerText = utc;
    }
    setInterval(updateClock, 1000);
    updateClock();

    // Tab switcher in Tactical Deck
    function switchDeckTab(tabKey, btn) {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById('tabContentProbe').style.display = tabKey === 'PROBE' ? 'block' : 'none';
      const sdrEl = document.getElementById('tabContentSDR');
      if (sdrEl) sdrEl.style.display = tabKey === 'SDR' ? 'block' : 'none';
      document.getElementById('tabContentLocate').style.display = tabKey === 'LOCATE' ? 'block' : 'none';
      document.getElementById('tabContentSAR').style.display = tabKey === 'SAR' ? 'block' : 'none';
      document.getElementById('tabContentRoster').style.display = tabKey === 'ROSTER' ? 'block' : 'none';
      document.getElementById('tabContentOPS').style.display = tabKey === 'OPS' ? 'block' : 'none';
    }

    function toggleTelemetryDock() {
      const dock = document.getElementById('bottomDock');
      const log = document.getElementById('eventLog');
      if (log.style.display === 'none') {
        log.style.display = 'block';
        dock.style.maxHeight = '140px';
      } else {
        log.style.display = 'none';
        dock.style.maxHeight = '32px';
      }
    }

    // High-Contrast CartoDB Dark Matter Maritime Basemap
    const map = L.map('map', { zoomControl: false }).setView([5.30, -0.85], 8);
    window.leafletMap = map;
    L.control.zoom({ position: 'topright' }).addTo(map);

    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}', {
      attribution: '&copy; Esri &bull; GEBCO, NOAA, National Geographic, Garmin &bull; GH-MARSAR C4ISR',
      maxZoom: 16
    }).addTo(map);

    function logEvent(msg, type = 'event-info') {
      const box = document.getElementById('eventLog');
      const time = new Date().toLocaleTimeString('en-GB');
      const line = document.createElement('div');
      line.className = type;
      line.innerHTML = `<span style="color:#64748b;">[${time}]</span> ${msg}`;
      box.insertBefore(line, box.firstChild);
    }

    function zoomSector(sector, btn) {
      if (btn) {
        document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
      }
      switch (sector) {
        case 'ALL': map.flyTo([5.30, -0.85], 8); break;
        case 'WESTERN': map.flyTo([4.85, -2.10], 10); break;
        case 'CENTRAL': map.flyTo([5.15, -1.15], 10); break;
        case 'ACCRA': map.flyTo([5.65, -0.10], 11); break;
        case 'VOLTA': map.flyTo([5.85, 0.95], 11); break;
      }
    }

    const vesselMarkers = {};
    const sarMarkers = {};

    // Custom SVG Maritime Vessel Icons (IEC 62288)
    function createVesselIcon(isCanoe, status, heading) {
      if (isCanoe) {
        const isSOS = status === 'SOS_DISTRESS' || status === 'OVERDUE';
        const isSea = status === 'AT_SEA';
        const color = isSOS ? '#ef4444' : (isSea ? '#38bdf8' : '#10b981');
        const glow = isSOS ? 'filter:drop-shadow(0 0 8px #ef4444);' : (isSea ? 'filter:drop-shadow(0 0 6px #38bdf8);' : '');
        return L.divIcon({
          className: 'canoe-marker',
          html: `<div style="transform:rotate(${heading||0}deg);${glow}">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <path d="M12 2L4 20L12 16L20 20L12 2Z" fill="${color}" stroke="#ffffff" stroke-width="1.5" stroke-linejoin="round"/>
            </svg>
          </div>`,
          iconSize: [24, 24],
          iconAnchor: [12, 12]
        });
      } else {
        // Commercial Vessel Hull Silhouette
        return L.divIcon({
          className: 'tanker-marker',
          html: `<div style="transform:rotate(${heading||0}deg);filter:drop-shadow(0 0 8px rgba(245,158,11,0.6));">
            <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
              <rect x="9" y="4" width="10" height="20" rx="3" fill="#f59e0b" stroke="#ffffff" stroke-width="1.5"/>
              <polygon points="14,1 9,8 19,8" fill="#f59e0b" stroke="#ffffff" stroke-width="1.5"/>
              <circle cx="14" cy="18" r="2" fill="#030712"/>
            </svg>
          </div>`,
          iconSize: [28, 28],
          iconAnchor: [14, 14]
        });
      }
    }

    async function initNationalGhanaBoundaries() {
      // 1. Ghana 6NM Inshore Exclusion Zone (IEZ - Fisheries Act 625)
      const iezPoints = [
        [5.1000, -3.1000], [4.7400, -2.0900], [4.8000, -1.9500], [4.8900, -1.7400],
        [5.0800, -1.3500], [5.3500, -0.6200], [5.5300, -0.2100], [5.6200, 0.0100],
        [5.7800, 0.6300], [5.8000, 0.9000], [6.1100, 1.2000],
        [5.9900, 1.2500], [5.6800, 0.9500], [5.6600, 0.6700], [5.5000, 0.0500],
        [5.4100, -0.1700], [5.2300, -0.5800], [4.9600, -1.3100], [4.7700, -1.7000],
        [4.6800, -1.9100], [4.6200, -2.0500], [4.9800, -3.0600]
      ];
      L.polygon(iezPoints, {
        color: '#0284c7',
        weight: 1.5,
        opacity: 0.8,
        fillColor: '#0284c7',
        fillOpacity: 0.04,
        dashArray: '6, 6'
      }).addTo(map).bindPopup('<b>Ghana 6NM Inshore Exclusion Zone (IEZ)</b><br>Artisanal canoes only. Industrial trawlers strictly prohibited (Fisheries Act 625).');

      // 2. Render 29 Coastal Cell Towers
      try {
        const tResp = await fetch('/v1/telecom/towers');
        if (tResp.ok) {
          const tData = await tResp.json();
          tData.towers.forEach(t => {
            const isSeaward = t.sector_type === 'SEAWARD';
            const color = isSeaward ? '#06b6d4' : '#64748b';
            L.circleMarker([t.lat, t.lon], {
              radius: 4,
              fillColor: color,
              color: '#ffffff',
              weight: 1,
              fillOpacity: 0.9
            }).addTo(map).bindPopup(`<b>${t.name}</b><br>Sector: ${t.sector_type} (${t.azimuth_deg}&deg;)<br>Region: ${t.region}`);
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

          const datalist = document.getElementById('canoeSuggestions');
          if (datalist) {
            datalist.innerHTML = data.subscribers.map(s => `<option value="${s.phone_number}">${s.canoe_name} (${s.landing_beach})</option>`).join('');
          }

          let atSea = 0;
          let onShore = 0;
          let sosCount = 0;

          if (data.subscribers.length === 0) {
            document.getElementById('fleetTable').innerHTML = '<div style="color:var(--text-faint);padding:6px 0;">No artisanal canoes enrolled yet.</div>';
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
              <div style="border-bottom:1px solid rgba(255,255,255,0.06);padding:6px 0;display:flex;justify-content:space-between;align-items:center;">
                <div>
                  <strong style="color:#f8fafc;">${s.canoe_name}</strong> <span style="color:var(--text-faint);font-size:0.68rem;">(${s.phone_number})</span><br>
                  <span style="color:var(--text-muted);font-size:0.66rem;">${s.landing_beach} &bull; ${s.language}</span>
                </div>
                <div style="text-align:right;display:flex;gap:3px;align-items:center;">
                  <span style="background:${badgeColor};color:#fff;padding:2px 5px;border-radius:3px;font-size:0.65rem;font-weight:700;">${s.status}</span>
                  ${s.status === 'ON_SHORE' ? `<button class="btn btn-sm" style="background:#0284c7;padding:2px 5px;" title="Simulate Launch" onclick="quickLaunch('${s.phone_number}', '${s.landing_beach}')">🚀</button>` : ''}
                  ${s.status === 'AT_SEA' ? `<button class="btn btn-sm btn-emerald" style="padding:2px 5px;" title="Return Safe" onclick="quickReturn('${s.phone_number}', '${s.landing_beach}')">🏖️</button>` : ''}
                  ${s.status !== 'SOS_DISTRESS' ? `<button class="btn btn-sm btn-danger" style="padding:2px 5px;" title="Distress SOS" onclick="quickSOS('${s.phone_number}')">🚨</button>` : ''}
                  <button class="btn btn-sm" style="background:#0891b2;padding:2px 5px;" title="Real-time GSM Locate" onclick="locateSpecific('${s.phone_number}')">🔍</button>
                  <button class="btn btn-sm" style="background:#475569;padding:2px 5px;" title="Unenroll" onclick="unenrollCanoe('${s.phone_number}')">🗑️</button>
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
            document.getElementById('activeSARBox').innerHTML = '<div style="color:var(--text-faint);">No active missing boat incidents. Coast is clear.</div>';
            return;
          }

          const html = data.incidents.map(inc => {
            const datum = inc.projected_datum;
            if (!sarMarkers[inc.incident_id]) {
              const marker = L.circleMarker([datum.lat, datum.lon], {
                radius: 8,
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

              let driftLine = null;
              if (inc.last_known_fix && inc.last_known_fix.lat) {
                driftLine = L.polyline([
                  [inc.last_known_fix.lat, inc.last_known_fix.lon],
                  [datum.lat, datum.lon]
                ], { color: '#f59e0b', weight: 2, dashArray: '6, 6' }).addTo(map).bindPopup(`<b>Guinea Current Drift Track</b><br>${inc.canoe_name} carried ${inc.drift_distance_nm} NM Eastward towards Togo`);
              }
              sarMarkers[inc.incident_id] = { marker, circle, driftLine };
            }

            return `
              <div style="border-bottom:1px solid rgba(255,255,255,0.08);padding:6px 0;display:flex;justify-content:space-between;align-items:center;">
                <div>
                  <strong style="color:#f87171;">🚨 ${inc.canoe_name}</strong> <span style="font-size:0.68rem;color:var(--text-faint);">(${inc.landing_beach})</span><br>
                  <span style="font-size:0.66rem;color:#fbbf24;">Datum: [${datum.lat.toFixed(3)}, ${datum.lon.toFixed(3)}] &bull; ${inc.drift_distance_nm} NM adrift</span>
                </div>
                <button class="btn btn-sm btn-emerald" style="padding:2px 8px;" onclick="resolveSAR('${inc.incident_id}')">Found</button>
              </div>
            `;
          }).join('');
          document.getElementById('activeSARBox').innerHTML = html;
        }
      } catch (e) {}
    }

    
    
    let sdrDroneMarker = null;
    let sdrTargetMarker = null;
    let sdrBeamCone = null;

    async function executeSDRScan() {
      const phone = document.getElementById('sdrPhoneInput').value.trim() || '0243875318';
      const station = document.getElementById('sdrStationSelect').value;

      try {
        const resp = await fetch('/v1/sdr/scan', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            phone_number: phone,
            search_station_id: station,
            band: 'GSM_900',
            force_simulated_detection: true
          })
        });

        if (resp.ok) {
          const res = await resp.json();
          const t = res.target;
          const card = document.getElementById('sdrResultCard');
          card.style.display = 'block';

          document.getElementById('sdrTimeSpan').innerText = t.timestamp_iso.split(' ')[1] || '';
          document.getElementById('sdrTelemetryText').innerHTML = `
            <strong>${res.canoe_name} (${t.phone_number})</strong><br>
            🎯 <strong>EXTERNAL FIX:</strong> [${t.lat.toFixed(4)}, ${t.lon.toFixed(4)}] &plusmn;${t.accuracy_m}m<br>
            📡 <strong>STATION:</strong> ${t.detected_via_station}<br>
            ⚡ <strong>RF POWER:</strong> ${t.rssi_dbm} dBm (Confidence: ${(t.confidence_score*100).toFixed(0)}%)<br>
            ⏱️ <strong>RANGE / BEARING:</strong> ${t.estimated_distance_m}m &bull; ${t.bearing_deg}&deg;<br>
            🔒 <strong>METHOD:</strong> 3GPP RACH Uplink Time-of-Flight (Zero Links/Apps)
          `;

          if (res.closest_commercial_ship) {
            const ship = res.closest_commercial_ship;
            document.getElementById('sdrShipAdvisory').innerHTML = `
              🚨 <strong>COMMERCIAL SHIP RENDEZVOUS:</strong><br>
              Vessel MMSI <strong>${ship.mmsi}</strong> is ${ship.distance_nm} NM away (Bearing ${ship.bearing_deg}&deg;).<br>
              VHF Ch 16 Automated Advisory Broadcast to intercept canoe!
            `;
            logEvent(`🚨 [SDR SAR RENDEZVOUS] Commercial vessel MMSI ${ship.mmsi} dispatched to intercept ${res.canoe_name} at [${t.lat.toFixed(4)}, ${t.lon.toFixed(4)}]!`, 'event-cpa');
          } else {
            document.getElementById('sdrShipAdvisory').style.display = 'none';
          }

          logEvent(`📡 [EXTERNAL SDR FIX] Target ${t.phone_number} acquired at [${t.lat.toFixed(4)}, ${t.lon.toFixed(4)}] via RF Time-of-Flight!`, 'event-lbs');

          // Plot on Leaflet Map
          if (window.leafletMap) {
            if (sdrTargetMarker) window.leafletMap.removeLayer(sdrTargetMarker);
            if (sdrDroneMarker) window.leafletMap.removeLayer(sdrDroneMarker);
            if (sdrBeamCone) window.leafletMap.removeLayer(sdrBeamCone);

            // Drone/station marker
            const stationCoords = station === 'DRONE_KETA_SAR_1' ? [5.8750, 1.0400] : [5.8450, 0.9950];
            sdrDroneMarker = L.circleMarker(stationCoords, {
              radius: 10,
              fillColor: '#38bdf8',
              color: '#fff',
              weight: 2,
              fillOpacity: 0.9
            }).addTo(window.leafletMap).bindPopup(`<b>${t.detected_via_station}</b><br>SDR Active Search Array`);

            // Target marker with pulsing ring
            sdrTargetMarker = L.circleMarker([t.lat, t.lon], {
              radius: 12,
              fillColor: '#ef4444',
              color: '#fbbf24',
              weight: 3,
              fillOpacity: 0.8
            }).addTo(window.leafletMap).bindPopup(`<b>${res.canoe_name}</b><br>RF Pocket Handset Intercept<br>[${t.lat.toFixed(4)}, ${t.lon.toFixed(4)}]`);

            // Beam line connecting drone to target
            sdrBeamCone = L.polyline([stationCoords, [t.lat, t.lon]], {
              color: '#38bdf8',
              weight: 2,
              dashArray: '4, 6',
              opacity: 0.85
            }).addTo(window.leafletMap);

            window.leafletMap.flyTo([t.lat, t.lon], 11);
          }
        }
      } catch (e) {
        console.error('SDR Scan failed:', e);
      }
    }

    async function executeSDRTriangulate() {
      const phone = document.getElementById('sdrPhoneInput').value.trim() || '0243875318';
      try {
        const resp = await fetch('/v1/sdr/triangulate', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ phone_number: phone })
        });
        if (resp.ok) {
          const res = await resp.json();
          logEvent(`📐 [TRIANGULATION] Multi-station fix established for ${res.phone_number}: [${res.lat}, ${res.lon}] (&plusmn;${res.accuracy_m}m)`, 'event-lbs');
          executeSDRScan();
        }
      } catch (e) {}
    }

    async function runSilentProbe(mode) {
      const phone = document.getElementById('probePhoneInput').value.trim();
      if (!phone) return;
      let url = '/v1/probe/silent-ping';
      if (mode === 'CAPSIZE') url = '/v1/probe/simulate-capsize';
      if (mode === 'TOGO') url = '/v1/probe/simulate-roaming';
      if (mode === 'RESET') url = '/v1/probe/reset';

      try {
        const resp = await fetch(url, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ phone_number: phone })
        });
        if (resp.ok) {
          const data = await resp.json();
          const pr = data.probe_result;
          const box = document.getElementById('probeResultBox');
          box.style.display = 'block';

          const badge = document.getElementById('probeStatusBadge');
          const summary = document.getElementById('probeSummaryText');
          const trace = document.getElementById('probeTraceBox');

          if (pr.status === 'RINGING_ALIVE') {
            badge.style.background = 'rgba(16,185,129,0.2)';
            badge.style.color = '#10b981';
            badge.style.borderColor = '#10b981';
            badge.innerText = '🟢 RINGING_ALIVE (SIP 180)';
            summary.innerHTML = '<strong>' + pr.carrier_name + '</strong> &bull; Handset connected to cell tower. Ring-and-drop canceled in ' + pr.latency_ms + 'ms. Cost: GH₵ 0.00.';
            logEvent('[SIP PROBE] ' + pr.phone_number + ' on ' + pr.carrier_name + ': ACTIVE. 0-second ring-and-drop successful.', 'event-lbs');
          } else if (pr.status === 'IMPLICIT_DETACH_ABSENT') {
            badge.style.background = 'rgba(239,68,68,0.2)';
            badge.style.color = '#f87171';
            badge.style.borderColor = '#ef4444';
            badge.innerText = '🚨 WATER IMMERSION / ABSENT (SIP 480)';
            summary.innerHTML = '<strong style="color:#ef4444;">ABNORMAL RADIO LINK FAILURE!</strong> Phone drowned in water. Watchdog automatically calculated Guinea Current drift datum!';
            const datumStr = data.drift_datum ? '[' + data.drift_datum.lat.toFixed(3) + ', ' + data.drift_datum.lon.toFixed(3) + ']' : '';
            logEvent('🚨 [WATER IMMERSION DETECTED] ' + pr.phone_number + ' lost signal abruptly! SAR Watchdog calculated drift datum ' + datumStr + ' towards Togo!', 'event-cpa');
            refreshSAR();
            if (data.drift_datum && window.leafletMap) {
              window.leafletMap.setView([data.drift_datum.lat, data.drift_datum.lon], 9);
            }
          } else if (pr.status === 'ROAMING_BORDER_CROSS') {
            badge.style.background = 'rgba(245,158,11,0.2)';
            badge.style.color = '#fbbf24';
            badge.style.borderColor = '#f59e0b';
            badge.innerText = '🇹🇬 BORDER CROSSING (Togocom MCC 615)';
            summary.innerHTML = '<strong style="color:#fbbf24;">VESSEL ADRIFT IN TOGO WATERS!</strong> SIM silently attached to Togocom Lomé cell tower. International rescue alert dispatched.';
            logEvent('⚠️ [INTERNATIONAL BORDER CROSS] ' + pr.phone_number + ' attached to Togocom (MCC 615)! Adrift into Togo territorial waters!', 'event-cpa');
            if (window.leafletMap) {
              window.leafletMap.setView([6.115, 1.235], 9);
            }
          }

          trace.innerText = pr.signaling_trace.join(String.fromCharCode(10));
        }
      } catch (e) {
        console.error(e);
      }
    }

    async function pollRadar() {
      try {
        const resp = await fetch('/v1/fleet/snapshot');
        if (resp.ok) {
          const snap = await resp.json();
          snap.vehicles.forEach(v => {
            const isCanoe = v.entity_id.startsWith('canoe-');
            const pt = [v.position.lat, v.position.lon];
            const titleName = v.ship_name || (isCanoe ? "ARTISANAL CANOE" : "COMMERCIAL VESSEL");
            let popupHtml = "<b>" + (isCanoe ? "⛴ ARTISANAL CANOE" : "🚢 " + titleName) + "</b><br>" +
              "ID: <b>" + v.entity_id + "</b><br>" +
              "Speed: " + (v.speed_mps * 1.94384).toFixed(1) + " knots<br>" +
              "Heading: " + v.heading_deg.toFixed(0) + "&deg;<br>" +
              "Position: [" + v.position.lat.toFixed(4) + ", " + v.position.lon.toFixed(4) + "]<br>";
            if (v.is_live) {
              popupHtml += '<span style="color:#10b981;font-weight:700;">🟢 LIVE SATELLITE/VHF AIS</span>';
            }
            if (vesselMarkers[v.entity_id]) {
              vesselMarkers[v.entity_id].setLatLng(pt);
              vesselMarkers[v.entity_id].setIcon(createVesselIcon(isCanoe, v.status, v.heading_deg));
              vesselMarkers[v.entity_id].setPopupContent(popupHtml);
            } else {
              const marker = L.marker(pt, {
                icon: createVesselIcon(isCanoe, v.status, v.heading_deg)
              }).addTo(map);
              marker.bindPopup(popupHtml);
              vesselMarkers[v.entity_id] = marker;
            }
          });

          // Render ARPA Collision Line if hazards exist
          if (snap.collision_hazards && snap.collision_hazards.length > 0) {
            snap.collision_hazards.forEach(h => {
              const tanker = vesselMarkers[h.threat_vessel];
              const canoe = vesselMarkers[h.canoe_id];
              if (tanker && canoe) {
                if (!window.hazardLine) {
                  window.hazardLine = L.polyline([tanker.getLatLng(), canoe.getLatLng()], {
                    color: '#ef4444',
                    weight: 3,
                    dashArray: '5, 5'
                  }).addTo(map);
                } else {
                  window.hazardLine.setLatLngs([tanker.getLatLng(), canoe.getLatLng()]);
                }
              }
              logEvent(`🚨 [COLLISION CONVERGENCE] Tanker ${h.threat_vessel} converging on ${h.canoe_name}! CPA: ${h.cpa_nm} NM in ${h.tcpa_minutes}m! Robocall dispatched in ${h.language}: "${h.voice_script}"`, 'event-cpa');
            });
          } else if (window.hazardLine) {
            map.removeLayer(window.hazardLine);
            window.hazardLine = null;
          }

          const shipsCount = snap.vehicles.filter(v => !v.entity_id.startsWith('canoe-')).length;
          const shipBadge = document.getElementById('statShips');
          if (shipBadge) shipBadge.innerText = shipsCount;
          const aisBadge = document.getElementById('liveAISBadge');
          if (aisBadge && snap.live_ais_status) {
            aisBadge.innerText = snap.live_ais_status + ' (' + (snap.live_ais_messages || 0) + ' msgs)';
          }
        }
      } catch (e) {}
    }

    setInterval(pollRadar, 3000);
    setInterval(refreshRoster, 5000);
    setInterval(refreshSAR, 6000);

    async function locateCanoeRealtime() {
      const input = document.getElementById('locateInput').value.trim();
      if (!input) {
        alert('Please enter a phone number or canoe name to locate.');
        return;
      }
      try {
        logEvent(`[GSM PING] Interrogating carrier HLR/VLR for ${input}...`, 'event-info');
        const resp = await fetch(`/v1/telecom/locate/${encodeURIComponent(input)}`);
        if (!resp.ok) {
          alert(`Canoe '${input}' not found in registry.`);
          return;
        }
        const data = await resp.json();
        const box = document.getElementById('locateResultBox');
        box.style.display = 'block';

        const isRelocated = data.safety_assessment === 'SAFE_RELOCATED_LANDING';
        const isSafeHome = data.safety_assessment === 'SAFE_ON_SHORE_HOME';
        const isSea = data.safety_assessment === 'ACTIVE_AT_SEA';
        const isSOS = data.safety_assessment === 'SOS_DISTRESS';

        const badgeColor = isSOS ? '#dc2626' : ((isRelocated || isSafeHome) ? '#059669' : '#0284c7');
        const badgeTitle = isRelocated ? `🟢 SAFE ON SHORE &bull; ${data.nearest_beach.toUpperCase()} (${data.distance_from_home_km}km from ${data.home_beach})` :
                           (isSafeHome ? `🟢 SAFE ON SHORE &bull; ${data.home_beach.toUpperCase()}` :
                           (isSea ? `🔵 AT SEA &bull; ${data.distance_offshore_km}km OFFSHORE` : `🔴 EMERGENCY SOS`));

        box.innerHTML = `
          <div style="background:${badgeColor};color:#fff;padding:4px 8px;border-radius:4px;font-weight:700;font-size:0.72rem;margin-bottom:6px;">
            ${badgeTitle}
          </div>
          <div style="color:#f8fafc;font-weight:600;margin-bottom:3px;">${data.canoe_name} (${data.phone_number})</div>
          <div style="color:#cbd5e1;margin-bottom:4px;font-size:0.7rem;">${data.display_message_en}</div>
          <div style="color:#38bdf8;font-style:italic;margin-bottom:6px;font-size:0.68rem;background:#030712;padding:6px;border-radius:4px;border-left:3px solid #38bdf8;">
            🇬🇭 Mother Tongue: "${data.display_message_local}"
          </div>
          <div style="display:flex;justify-content:space-between;color:var(--text-faint);font-size:0.66rem;border-top:1px solid rgba(255,255,255,0.06);padding-top:4px;">
            <span>Tower: <strong>${data.tower_name}</strong></span>
            <span>Timing Adv: <strong>TA=${data.timing_advance}</strong> (${data.distance_offshore_km}km)</span>
            <span>Ping: <strong>${data.ping_latency_ms}ms</strong></span>
          </div>
        `;

        const coords = data.current_coordinates;
        map.flyTo([coords.lat, coords.lon], 13);

        if (window.locateMarker) map.removeLayer(window.locateMarker);
        if (window.relocationLine) map.removeLayer(window.relocationLine);

        const markerColor = isSOS ? '#dc2626' : ((isRelocated || isSafeHome) ? '#10b981' : '#38bdf8');
        window.locateMarker = L.circleMarker([coords.lat, coords.lon], {
          radius: 10,
          fillColor: markerColor,
          color: '#ffffff',
          weight: 3,
          fillOpacity: 0.95
        }).addTo(map).bindPopup(`
          <strong>${data.canoe_name} (${data.phone_number})</strong><br>
          Status: <b>${badgeTitle}</b><br>
          Current Location: ${data.nearest_beach}<br>
          Home Beach: ${data.home_beach} (${data.distance_from_home_km} km away)<br>
          Tower: ${data.tower_name}<br>
          Timing Advance: TA=${data.timing_advance}
        `).openPopup();

        if (isRelocated) {
          const homeCoords = [5.9200, 0.9900]; // Keta default
          window.relocationLine = L.polyline([homeCoords, [coords.lat, coords.lon]], {
            color: '#10b981',
            weight: 3,
            dashArray: '6, 6'
          }).addTo(map).bindPopup(`<b>Relocation Flight Path</b><br>Launched from ${data.home_beach} &rarr; Landed at ${data.nearest_beach} (${data.distance_from_home_km} km)`);
        }

        logEvent(`[REALTIME GSM FIX] ${data.canoe_name} (${data.phone_number}) located at ${data.nearest_beach}. ${isRelocated ? 'Safe on shore at another beach - false missing boat alarm avoided!' : 'Tracked successfully.'}`, isRelocated ? 'event-enter' : 'event-info');
      } catch (e) {
        console.error(e);
      }
    }

    function locateSpecific(phone) {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelector('.tab-btn').classList.add('active');
      document.getElementById('tabContentLocate').style.display = 'block';
      document.getElementById('tabContentSAR').style.display = 'none';
      document.getElementById('tabContentRoster').style.display = 'none';
      document.getElementById('tabContentOPS').style.display = 'none';
      document.getElementById('locateInput').value = phone;
      locateCanoeRealtime();
    }

    async function simulateKetaToVodza() {
      const subsResp = await fetch('/v1/telecom/subscribers');
      const subsData = await subsResp.json();
      let match = subsData.subscribers.find(s => s.landing_beach.includes('Keta') || s.canoe_name.includes('Dzidula'));
      const phone = match ? match.phone_number : '+233240003344';

      logEvent(`[SIMULATION] Fisherman with canoe '${match ? match.canoe_name : "Dzidula"}' left Keta and landed at Vodza Beach to sell catch...`, 'event-sar');
      const resp = await fetch(`/v1/telecom/ping/${encodeURIComponent(phone)}?simulated_tower=TOWER_VODZA_COASTAL&timing_advance=0`, {
        method: 'POST'
      });
      if (resp.ok) {
        document.getElementById('locateInput').value = phone;
        await locateCanoeRealtime();
        await refreshRoster();
      }
    }

    async function simulateReturnToKeta() {
      const subsResp = await fetch('/v1/telecom/subscribers');
      const subsData = await subsResp.json();
      let match = subsData.subscribers.find(s => s.landing_beach.includes('Keta') || s.canoe_name.includes('Dzidula'));
      const phone = match ? match.phone_number : '+233240003344';

      logEvent(`[SIMULATION] Canoe '${match ? match.canoe_name : "Dzidula"}' returns back to home beach at Keta...`, 'event-info');
      const resp = await fetch(`/v1/telecom/ping/${encodeURIComponent(phone)}?simulated_tower=TOWER_KETA_INLAND&timing_advance=0`, {
        method: 'POST'
      });
      if (resp.ok) {
        document.getElementById('locateInput').value = phone;
        await locateCanoeRealtime();
        await refreshRoster();
      }
    }

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
        if (sarMarkers[incId].driftLine) map.removeLayer(sarMarkers[incId].driftLine);
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
      logEvent(`[AUTOMATIC SEA LAUNCH] Canoe launched into deep sea. Offshore distance: ${data.distance_offshore_km} km. Zero dials required.`, 'event-enter');
      await refreshRoster();
      await pollRadar();
    }

    async function injectAISVessel() {
      const subsResp = await fetch('/v1/telecom/subscribers');
      const subsData = await subsResp.json();
      let activeCanoe = subsData.subscribers.find(s => s.status === 'AT_SEA' && s.last_point);

      if (!activeCanoe && subsData.subscribers.length > 0) {
        const cPhone = subsData.subscribers[0].phone_number;
        const bch = subsData.subscribers[0].landing_beach || 'Keta Beach';
        await quickLaunch(cPhone, bch);
        const refResp = await fetch('/v1/telecom/subscribers');
        const refData = await refResp.json();
        activeCanoe = refData.subscribers.find(s => s.phone_number === cPhone);
      }

      if (activeCanoe && activeCanoe.last_point) {
        const cLat = activeCanoe.last_point.lat;
        const cLon = activeCanoe.last_point.lon;
        const tLat = cLat - 0.026;
        const tLon = cLon + 0.005;

        logEvent(`[AIS COLLISION INJECTION] Ingesting crude tanker (MMSI 636019842) converging 1.6 NM south of ${activeCanoe.canoe_name}...`, 'event-cpa');
        const resp = await fetch('/v1/ais/vessel/inject', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            mmsi: 636019842,
            vessel_name: 'CRUDE STAR (LIBERIA)',
            lat: tLat,
            lon: tLon,
            speed_knots: 16.5,
            heading_deg: 350.0
          })
        });
        const data = await resp.json();
        logEvent(`[AIS TANKER ENGAGED] ${data.vessel_name} heading directly toward ${activeCanoe.canoe_name}! ARPA calculating CPA/TCPA...`, 'event-cpa');
        map.flyTo([cLat, cLon], 12);
      } else {
        logEvent('[AIS INGESTION] Ingesting Class-A crude tanker at Tema Anchorage VHF...', 'event-info');
        const sample = '!AIVDM,1,1,,A,19Eu8C002A00>aP3=kp9TWb00000,0*64';
        const resp = await fetch('/v1/ais/nmea', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ raw_nmea: sample })
        });
        const data = await resp.json();
        logEvent(`[AIS TANKER MAPPED] MMSI ${data.mmsi} Speed: ${data.speed_knots}kt Heading: ${data.heading_deg}°`, 'event-info');
      }
      await pollRadar();
    }

    async function quickLaunch(phone, beach) {
      logEvent(`[LAUNCH] Simulating departure from ${beach} for ${phone}...`, 'event-info');
      const resp = await fetch('/v1/telecom/cell-event', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ phone_number: phone, tower_id: beach, timing_advance: 8 })
      });
      const data = await resp.json();
      logEvent(`[AT SEA] ${data.canoe_name} is now ${data.distance_offshore_km} km offshore via ${data.tower_name}.`, 'event-enter');
      await refreshRoster();
      await pollRadar();
    }

    async function quickReturn(phone, beach) {
      logEvent(`[RETURN] Simulating safe beach landing at ${beach} for ${phone}...`, 'event-info');
      const resp = await fetch('/v1/telecom/cell-event', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ phone_number: phone, tower_id: beach, timing_advance: 0 })
      });
      const data = await resp.json();
      logEvent(`[RETURNED SAFELY] ${data.canoe_name} safely landed on shore!`, 'event-enter');
      await refreshRoster();
      await pollRadar();
    }

    async function quickSOS(phone) {
      logEvent(`[KEYPAD MAYDAY] Incoming USSD/SMS SOS from ${phone}...`, 'event-cpa');
      const resp = await fetch('/v1/telecom/sos', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ phone_number: phone, message: 'MAYDAY_ENGINE_FAILURE' })
      });
      const data = await resp.json();
      logEvent(`🚨 [MAYDAY ACTIVATED]: ${data.canoe_name} triggered SOS! Navy rescue docket created.`, 'event-cpa');
      await refreshSAR();
      await refreshRoster();
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
