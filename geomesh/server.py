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


# -------------------------------------------------------------------------
# Embedded Interactive Leaflet.js Map Dashboard
# -------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
def get_map_dashboard() -> str:
    """Serve embedded interactive real-time Leaflet.js tracking dashboard."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>GeoMesh-Stream: Real-Time Fleet & Geofence Dashboard</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    body { margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #f8fafc; }
    #container { display: flex; height: 100vh; }
    #sidebar { width: 380px; background: #1e293b; padding: 20px; overflow-y: auto; border-right: 1px solid #334155; }
    #map { flex-grow: 1; height: 100%; }
    h1 { font-size: 1.15rem; margin-top: 0; color: #38bdf8; display: flex; align-items: center; justify-content: space-between; }
    .badge { font-size: 0.7rem; background: #0284c7; padding: 3px 8px; border-radius: 9999px; }
    .btn { background: #0284c7; color: white; border: none; padding: 8px 14px; border-radius: 6px; cursor: pointer; width: 100%; font-weight: 600; margin-bottom: 12px; }
    .btn:hover { background: #0369a1; }
    .card { background: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 12px; margin-bottom: 14px; }
    .card h3 { margin-top: 0; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.05em; color: #94a3b8; }
    .event-log { font-family: monospace; font-size: 0.75rem; max-height: 250px; overflow-y: auto; background: #020617; padding: 8px; border-radius: 4px; }
    .event-enter { color: #4ade80; }
    .event-exit { color: #f87171; }
  </style>
</head>
<body>
  <div id="container">
    <div id="sidebar">
      <h1>GeoMesh-Stream <span class="badge">Live Telemetry</span></h1>
      <p style="font-size: 0.8rem; color: #94a3b8;">High-Throughput Spatial Grid Indexing & Real-Time Geofencing Engine</p>
      
      <button class="btn" onclick="startSimulation()">Simulate Vehicle Fleet</button>

      <div class="card">
        <h3>Active Geofences</h3>
        <div id="fenceList" style="font-size: 0.8rem;">Loading geofences...</div>
      </div>

      <div class="card">
        <h3>Real-Time Breach Stream</h3>
        <div id="eventLog" class="event-log">Listening for telemetry pings...</div>
      </div>

      <div style="font-size: 0.75rem; color: #64748b; text-align: center;">
        Interactive OpenAPI docs at <a href="/docs" target="_blank" style="color: #38bdf8;">/docs</a>
      </div>
    </div>
    <div id="map"></div>
  </div>

  <script>
    const map = L.map('map').setView([5.6037, -0.1870], 13);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      maxZoom: 19,
      attribution: '&copy; CartoDB &copy; OpenStreetMap'
    }).addTo(map);

    const vehicleMarkers = {};
    const logBox = document.getElementById('eventLog');

    // Register initial demonstration geofences
    async function initDemo() {
      // 1. Kotoka Airport Radial Zone
      await fetch('/v1/geofences/radial', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          geofence_id: 'kotoka-airport',
          name: 'Kotoka Airport Zone',
          lat: 5.6050,
          lon: -0.1700,
          radius_m: 1200
        })
      });

      L.circle([5.6050, -0.1700], {
        color: '#38bdf8',
        fillColor: '#0284c7',
        fillOpacity: 0.2,
        radius: 1200
      }).addTo(map).bindPopup('<b>Kotoka Airport Zone</b><br>Radius: 1,200m');

      document.getElementById('fenceList').innerHTML = '<div>• <b>Kotoka Airport</b> (Radial 1.2km)</div>';
    }
    initDemo();

    function logEvent(text, type) {
      const line = document.createElement('div');
      line.className = type === 'ENTER' ? 'event-enter' : 'event-exit';
      line.textContent = `[${new Date().toLocaleTimeString()}] ${text}`;
      logBox.prepend(line);
    }

    let simInterval = null;
    let simStep = 0;

    function startSimulation() {
      if (simInterval) clearInterval(simInterval);
      logBox.innerHTML = '<div>Simulation started...</div>';

      simInterval = setInterval(async () => {
        simStep++;
        // Vehicle travels toward and through airport geofence
        const lat = 5.6000 + (Math.sin(simStep * 0.15) * 0.012);
        const lon = -0.1900 + (simStep * 0.001);

        const resp = await fetch('/v1/telemetry', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            entity_id: 'haulage-truck-01',
            lat: lat,
            lon: lon,
            speed_mps: 18.5,
            heading_deg: 75.0
          })
        });

        const data = await resp.json();
        
        // Update Marker
        if (!vehicleMarkers['haulage-truck-01']) {
          vehicleMarkers['haulage-truck-01'] = L.circleMarker([lat, lon], {
            radius: 8,
            fillColor: '#f59e0b',
            color: '#fff',
            weight: 2,
            fillOpacity: 0.9
          }).addTo(map).bindPopup('<b>Haulage Truck 01</b>');
        } else {
          vehicleMarkers['haulage-truck-01'].setLatLng([lat, lon]);
        }

        // Log alerts
        if (data.alerts && data.alerts.length > 0) {
          data.alerts.forEach(a => {
            logEvent(`${a.event} ${a.geofence_id} (Dwell: ${a.dwell_duration_sec.toFixed(1)}s)`, a.event);
          });
        }
      }, 1000);
    }
  </script>
</body>
</html>
"""
