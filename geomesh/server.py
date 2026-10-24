"""
GeoMesh-Stream: Standalone Fleet Telemetry & Spatial Geofence Server.
Includes REST API endpoints and an embedded interactive Leaflet.js web dashboard.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field

from geomesh.engine import GeoMeshEngine
from geomesh.geofence import PolygonGeofence, RadialGeofence
from geomesh.types import (
    GeofenceAlreadyExistsError,
    GeofenceNotFoundError,
    InvalidPolygonError,
    Point,
    TelemetryPing,
)

app = FastAPI(
    title="GeoMesh-Stream Telemetry & Geofence Engine",
    description="High-Throughput Streaming Geospatial Engine with Spatial Grid Indexing, Kinematics & Dead-Reckoning.",
    version="1.0.0",
)

# Global engine instance
_engine = GeoMeshEngine(cell_size_deg=0.02)
_telemetry_pings_total = 0
_geofence_breaches_total = 0


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


@app.post("/v1/fleet/snapshot", tags=["Spatial Queries"])
def snapshot_fleet() -> Dict[str, Any]:
    """Capture point-in-time state checkpoint of all tracked fleet vehicles, positions, kinematics, and active breaches."""
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
# Embedded Interactive Leaflet.js Map Dashboard
# -------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
def get_map_dashboard() -> str:
    """Serve embedded interactive real-time Ghana Maritime tracking dashboard."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>GeoMesh-Stream: Ghana Maritime Collision Avoidance & Coastal Radar</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    body { margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #070d18; color: #f8fafc; }
    #container { display: flex; height: 100vh; }
    #sidebar { width: 400px; background: #0f172a; padding: 22px; overflow-y: auto; border-right: 1px solid #1e293b; box-shadow: 2px 0 10px rgba(0,0,0,0.5); }
    #map { flex-grow: 1; height: 100%; }
    h1 { font-size: 1.15rem; margin-top: 0; color: #38bdf8; display: flex; align-items: center; justify-content: space-between; }
    .badge { font-size: 0.65rem; background: #0284c7; padding: 3px 8px; border-radius: 9999px; letter-spacing: 0.05em; font-weight: 700; }
    .btn { background: #0284c7; color: white; border: none; padding: 10px 14px; border-radius: 6px; cursor: pointer; width: 100%; font-weight: 600; font-size: 0.85rem; margin-bottom: 14px; transition: background 0.2s; }
    .btn:hover { background: #0369a1; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 6px; padding: 12px; margin-bottom: 14px; }
    .card h3 { margin-top: 0; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; color: #94a3b8; margin-bottom: 8px; }
    .event-log { font-family: monospace; font-size: 0.75rem; max-height: 260px; overflow-y: auto; background: #020617; padding: 10px; border-radius: 4px; line-height: 1.5; border: 1px solid #1e293b; }
    .event-enter { color: #f87171; font-weight: 600; }
    .event-exit { color: #fbbf24; }
    .event-cpa { color: #ef4444; font-weight: 700; }
    .event-info { color: #38bdf8; }
    .leaflet-tile { filter: invert(100%) hue-rotate(180deg) brightness(88%) contrast(90%); }
    .leaflet-container { background: #050b14; }
    .zone-pill { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 0.7rem; margin-right: 4px; margin-bottom: 4px; }
  </style>
</head>
<body>
  <div id="container">
    <div id="sidebar">
      <h1>GeoMesh-Stream <span class="badge">GULF OF GUINEA</span></h1>
      <p style="font-size: 0.78rem; color: #94a3b8; line-height: 1.4; margin-top: 4px;">
        Ghana Coastal Vessel Telemetry & Artisanal Safety Engine (Keta &bull; Tema &bull; Volta Estuary).
      </p>
      
      <button class="btn" onclick="startMaritimeSimulation()">Simulate Ghana Maritime Traffic</button>

      <div class="card">
        <h3>Protected Maritime Zones</h3>
        <div id="fenceList" style="font-size: 0.78rem; line-height: 1.6;">Loading Ghana maritime zones...</div>
      </div>

      <div class="card">
        <h3>Real-Time Collision & IEZ Incident Stream</h3>
        <div id="eventLog" class="event-log">Monitoring Gulf of Guinea shipping lanes...</div>
      </div>

      <div style="font-size: 0.72rem; color: #64748b; text-align: center; line-height: 1.4;">
        Ghana Maritime Authority &amp; Fisheries Commission API<br>
        OpenAPI docs at <a href="/docs" target="_blank" style="color: #38bdf8;">/docs</a> &bull; <a href="/metrics" target="_blank" style="color: #38bdf8;">/metrics</a>
      </div>
    </div>
    <div id="map"></div>
  </div>

  <script>
    // Center on Ghana coast (between Tema and Keta)
    const map = L.map('map').setView([5.78, 0.45], 10);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 18,
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    const vesselMarkers = {};
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
      }).addTo(map).bindPopup('<b>Ghana Inshore Exclusion Zone (IEZ)</b><br>6 Nautical Mile Artisanal Fishing Reserve (Fisheries Act 625)');

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
    }
    initGhanaMaritime();

    let simInterval = null;
    let simStep = 0;

    function startMaritimeSimulation() {
      if (simInterval) clearInterval(simInterval);
      logBox.innerHTML = '<div class="event-info">Ghana maritime traffic stream started...</div>';

      simInterval = setInterval(async () => {
        simStep++;

        // 1. Commercial Tanker MT African Runner (MMSI: 627001420)
        // Traveling WSW along shipping channel towards Tema
        const tankerLat = 5.7500 - (simStep * 0.0022);
        const tankerLon = 0.8500 - (simStep * 0.0065);
        const tankerResp = await fetch('/v1/telemetry', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            entity_id: 'tanker-african-runner',
            lat: tankerLat,
            lon: tankerLon,
            speed_mps: 8.2, // ~16 knots
            heading_deg: 245.0
          })
        });
        const tankerData = await tankerResp.json();

        // 2. Artisanal Canoe Keta-01 [Nyimdzii] (Local fishermen with smartphone GPS)
        // Night drift-fishing off Cape St. Paul / Keta
        const canoeLat = 5.8200 - (simStep * 0.0008);
        const canoeLon = 0.8100 + (simStep * 0.0004);
        const canoeResp = await fetch('/v1/telemetry', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            entity_id: 'canoe-keta-01',
            lat: canoeLat,
            lon: canoeLon,
            speed_mps: 1.8, // ~3.5 knots
            heading_deg: 155.0
          })
        });
        const canoeData = await canoeResp.json();

        // Update Map Markers
        if (!vesselMarkers['tanker-african-runner']) {
          vesselMarkers['tanker-african-runner'] = L.circleMarker([tankerLat, tankerLon], {
            radius: 9,
            fillColor: '#38bdf8',
            color: '#fff',
            weight: 2,
            fillOpacity: 0.95
          }).addTo(map).bindPopup('<b>MT African Runner</b><br>Crude Tanker (85,000 DWT)<br>Speed: 16.0 kt &bull; AIS Class A');
        } else {
          vesselMarkers['tanker-african-runner'].setLatLng([tankerLat, tankerLon]);
        }

        if (!vesselMarkers['canoe-keta-01']) {
          vesselMarkers['canoe-keta-01'] = L.circleMarker([canoeLat, canoeLon], {
            radius: 7,
            fillColor: '#10b981',
            color: '#fff',
            weight: 2,
            fillOpacity: 0.95
          }).addTo(map).bindPopup('<b>Canoe Keta-01 [Nyimdzii]</b><br>Artisanal Fishermen (Phone GPS)<br>Speed: 3.5 kt &bull; Cape St. Paul');
        } else {
          vesselMarkers['canoe-keta-01'].setLatLng([canoeLat, canoeLon]);
        }

        // Query CPA between Tanker and Canoe
        try {
          const cpaResp = await fetch(`/v1/maritime/cpa?vessel_1=tanker-african-runner&vessel_2=canoe-keta-01`);
          if (cpaResp.ok) {
            const cpaData = await cpaResp.json();
            if (cpaData.risk_level === 'CRITICAL_COLLISION_ALERT') {
              logEvent(`CRITICAL COLLISION ALERT: MT African Runner CPA ${cpaData.cpa_nautical_miles} NM with Canoe Keta-01 (TCPA: ${cpaData.tcpa_minutes}m)`, 'event-cpa');
            } else if (cpaData.risk_level === 'WARNING_CONVERGING') {
              logEvent(`CONVERGING: MT African Runner & Canoe Keta-01 CPA ${cpaData.cpa_nautical_miles} NM (TCPA: ${cpaData.tcpa_minutes}m)`, 'event-exit');
            }
          }
        } catch (e) {}

        // Log geofence perimeter events
        if (tankerData.alerts && tankerData.alerts.length > 0) {
          tankerData.alerts.forEach(a => {
            logEvent(`[TANKER] ${a.event} on ${a.geofence_id} (Dwell: ${a.dwell_duration_sec.toFixed(0)}s)`, a.event === 'ENTER' ? 'event-enter' : 'event-exit');
          });
        }
        if (canoeData.alerts && canoeData.alerts.length > 0) {
          canoeData.alerts.forEach(a => {
            logEvent(`[CANOE] ${a.event} on ${a.geofence_id}`, 'event-info');
          });
        }
      }, 1200);
    }
  </script>
</body>
</html>
"""
