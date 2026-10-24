# GeoMesh-Stream

**Real-Time Ghana Maritime Vessel Stream, Collision Avoidance & Coastal Geofence Engine**

[![Build Status](https://img.shields.io/badge/tests-31%20passed-brightgreen.svg)](https://github.com/Huighlet/GeoMesh-Stream)
[![Python Version](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Zero AI Slop](https://img.shields.io/badge/codebase-zero%20ai%20slop-black.svg)](https://github.com/Huighlet/GeoMesh-Stream)

---

## Executive Summary

Coastal maritime environments in the **Gulf of Guinea** present an acute dual-class safety challenge: heavy industrial commercial vessels (crude oil tankers and container carriers calling at **Tema Port**) navigate shared waters alongside thousands of **Ghanaian artisanal wooden fishing canoes** (operating out of **Keta, Cape St. Paul, Ada Foah, and Prampram**). While commercial ships transmit Class-A AIS transponder telemetry, artisanal fishermen navigate with standard \$10 keypad feature phones ("yam phones") without GPS apps, smartphones, or data connectivity. Requiring daily manual dialing or USSD codes leads to non-compliance and fatal collision blindspots.

**GeoMesh-Stream** bridges this divide as an ultra-low latency, zero-dependency streaming geospatial engine written in Python 3.13. It unifies dual-class vessel telemetry into a high-performance **Discrete Spatial Grid Index ($\mathcal{O}(1)$ AABB candidate pruning)** and a **Zero-Touch Passive Telecom LBS Engine**:
1. **Zero-Touch Keypad Phone Safety:** Fishermen enroll once at the landing beach with their phone number, canoe name, and native language (**Ewe** in Keta, **Ga** in Jamestown, **Fante** in Elmina). Zero daily dialing is required.
2. **GSM Timing Advance (TA) Coordinate Derivation:** The passive telecom gateway derives offshore coordinates directly from cell tower radio propagation delays ($TA \times 550\text{m}$) and sector antenna azimuths (e.g., Cape St. Paul Seaward Sector).
3. **Passive Cell Handover Tripwires:** Handover from an inland sector to a seaward sector automatically transitions the canoe from `ON_SHORE` to `AT_SEA`. Handover back inland automatically registers `RETURNED_SAFELY`.
4. **Closest Point of Approach (CPA & TCPA):** Computes relative kinematic vectors to detect collision trajectories when industrial tankers converge within 1.0 Nautical Mile of artisanal canoes.
5. **Mother-Tongue IVR Robocalls:** Automatically triggers instant audible phone calls in the fisherman's native dialect (Ewe/Ga/Fante) when CPA risk thresholds are breached.
6. **Ghana Inshore Exclusion Zone (IEZ) Enforcement:** Mathematically evaluates vessel boundaries against the 6-Nautical-Mile artisanal reserve codified under the **Ghana Fisheries Act (Act 625)** to detect illegal industrial trawler incursions.
7. **Guinea Current Search-and-Rescue (SAR) Dead-Reckoning:** Uses great-circle kinematics and prevailing eastward ocean current vectors ($\approx 1.4\text{ kt}$) to project search-and-rescue drift cones when artisanal phone signals drop offline.

---

## System Architecture

```
                 DUAL-CLASS MARITIME TELEMETRY INGESTION
     [AIS Class-A Tankers / Cargo]          [Artisanal Phone GPS Pings]
      (MMSI, SOG, COG, Heading, ts)          (Canoe ID, Lat, Lon, Battery, ts)
                   \                                 /
                    \                               /
                     v                             v
     +---------------------------------------------------------------------------------------+
     |                                   GeoMeshEngine                                       |
     |                                                                                       |
     |  +------------------------------+             +------------------------------------+  |
     |  |       KinematicTracker       |             |          SpatialGridIndex          |  |
     |  |------------------------------|             |------------------------------------|  |
     |  |  Velocity: v = Δd / Δt       |             |  Cell: (⌊lat/δ⌋, ⌊lon/δ⌋)          |  |
     |  |  Heading: Azimuth θ          |             |  Candidate Zones: O(1)             |  |
     |  |  Acceleration: a = Δv / Δt   |             |  AABB Maritime Overlap Pruning     |  |
     |  |  CPA / TCPA Collision Vectors|             |                                    |  |
     |  +------------------------------+             +------------------------------------+  |
     |                 |                                                |                    |
     +-----------------|------------------------------------------------|--------------------+
                       |                                                |
                       v                                                v
     +----------------------------------+            +---------------------------------------+
     | Guinea Current Drift (SAR Cone)  |            |           GeofenceEngine              |
     | (Extrapolates Adrift Canoes)     |            |---------------------------------------|
     +----------------------------------+            | Radial: Tema Commercial Anchorage     |
                                                     | Polygon: Ghana 6NM IEZ (Act 625)      |
                                                     | Polygon: Keta Artisanal Grounds       |
                                                     | Line: Ghana - Togo Maritime Border    |
                                                     | State: ENTER / DWELL / EXIT           |
                                                     +---------------------------------------+
                                                                        |
                                                                        v
                                                   [Maritime Collision & Breach Stream]
```
```

---

## Mathematical Formulations

### 1. Great-Circle Haversine Distance

Given two geographic points on the WGS-84 reference sphere $P_1(\phi_1, \lambda_1)$ and $P_2(\phi_2, \lambda_2)$ where $\phi$ represents latitude and $\lambda$ represents longitude in radians:

$$\Delta\phi = \phi_2 - \phi_1, \quad \Delta\lambda = \lambda_2 - \lambda_1$$

$$a = \sin^2\left(\frac{\Delta\phi}{2}\right) + \cos(\phi_1)\cos(\phi_2)\sin^2\left(\frac{\Delta\lambda}{2}\right)$$

$$c = 2 \cdot \text{atan2}\left(\sqrt{a}, \ \sqrt{1 - a}\right)$$

$$d = R_{\text{earth}} \cdot c \quad \text{where } R_{\text{earth}} = 6,371,008.8\text{ meters}$$

### 2. Initial Forward Azimuth (Bearing)

The initial bearing $\theta \in [0^\circ, 360^\circ)$ along the great-circle arc from $P_1$ to $P_2$:

$$y = \sin(\Delta\lambda)\cos(\phi_2)$$

$$x = \cos(\phi_1)\sin(\phi_2) - \sin(\phi_1)\cos(\phi_2)\cos(\Delta\lambda)$$

$$\theta = \Big(\text{atan2}(y, x) \cdot \frac{180}{\pi} + 360\Big) \pmod{360}$$

### 3. Great-Circle Dead-Reckoning Position Projection

Given an asset at coordinate $(\phi_1, \lambda_1)$, traveling at velocity $v$ with acceleration $a$ along bearing $\theta$, the projected displacement distance $d$ over time elapsed $\Delta t$ is:

$$d = \max\Big(0, \ v \cdot \Delta t + \frac{1}{2} a (\Delta t)^2\Big), \quad \delta = \frac{d}{R_{\text{earth}}}$$

The projected latitude $\phi_2$ and longitude $\lambda_2$ are derived analytically:

$$\phi_2 = \arcsin\Big(\sin(\phi_1)\cos(\delta) + \cos(\phi_1)\sin(\delta)\cos(\theta)\Big)$$

$$\lambda_2 = \lambda_1 + \text{atan2}\Big(\sin(\theta)\sin(\delta)\cos(\phi_1), \ \cos(\delta) - \sin(\phi_1)\sin(\phi_2)\Big)$$

### 4. Ray-Casting Point-in-Polygon (Jordan Curve Theorem)

To determine whether an entity coordinate $P(x_0, y_0)$ lies within an arbitrary $V$-vertex polygon $\{V_1, V_2, \dots, V_n\}$:

A horizontal ray is cast eastward along the meridian:

$$\mathcal{R} = \{(x, y_0) \mid x \ge x_0\}$$

The intersection count $I$ with edge $(V_i, V_{i+1})$ is incremented if and only if:

$$(V_{i,y} > y_0) \ne (V_{i+1,y} > y_0) \quad \land \quad x_0 < \left(\frac{V_{i+1,x} - V_{i,x}}{V_{i+1,y} - V_{i,y}}(y_0 - V_{i,y}) + V_{i,x}\right)$$

$$\text{Contains}(P) \iff (I \pmod 2 \equiv 1)$$

---

## Microsecond Performance Benchmarks

Benchmarked on Python 3.13 standard runtime across 100,000 operations:

| Benchmark Phase | Operations / Sec | Mean Latency | Technical Guarantee |
| :--- | :--- | :--- | :--- |
| **Haversine Distance** | **382,666.6 calcs/sec** | **2.613 $\mu$s** | High-precision spherical metric |
| **Initial Forward Azimuth** | **404,066.7 calcs/sec** | **2.475 $\mu$s** | $[0.0^\circ, 360.0^\circ)$ normalized heading |
| **Ray-Casting Point-in-Polygon** | **466,025.8 checks/sec** | **2.146 $\mu$s** | Non-convex arbitrary boundary testing |
| **Spatial Grid Index Query** | **509,862.5 queries/sec** | **1.961 $\mu$s** | $\mathcal{O}(1)$ bounding box candidate retrieval |
| **Dead-Reckoning Projection** | **157,047.3 proj/sec** | **6.368 $\mu$s** | Kinematic spherical projection |
| **Full Engine Ingestion Pipeline** | **40,677.5 pings/sec** | **24.584 $\mu$s** | Telemetry, Kinematics & Alerts |

---

## Directory Structure

```
GeoMesh-Stream/
├── geomesh/
│   ├── __init__.py           # Package exports and module definitions
│   ├── types.py              # Point, BoundingBox, TelemetryPing, GeofenceAlert
│   ├── math.py               # Haversine, Bearing, Dead-Reckoning, Ray-Casting
│   ├── index.py              # Discrete Uniform Spatial Grid Hash Index
│   ├── geofence.py           # Radial & Polygon Geofences and State Machine
│   ├── kinematics.py         # Kinematic state estimator and CPA/TCPA math
│   ├── engine.py             # Unified streaming geospatial coordinator
│   ├── telecom.py            # Zero-touch passive GSM LBS, TA derivation & IVR voice alerts
│   ├── server.py             # Fast REST API, Prometheus metrics & Leaflet radar dashboard
│   ├── client.py             # Idiomatic GeoMeshClient SDK
│   └── cli.py                # Benchmark suite and CLI tool
├── examples/
│   └── simulate_ghana_maritime_mesh.py # End-to-end Ghana coastal simulation
├── tests/
│   ├── test_math.py          # Ground-truth trigonometry and polygon tests
│   ├── test_index.py         # Spatial cell hashing and radius search
│   ├── test_geofence.py      # ENTER/EXIT transitions and dwell duration
│   ├── test_kinematics.py    # Velocity, heading, and dead-reckoning checks
│   ├── test_engine.py        # Proximity queries and end-to-end ingestion
│   ├── test_server.py        # REST endpoints and live Leaflet dashboard
│   ├── test_telecom.py       # Zero-touch subscribers, Timing Advance & voice alerts
│   └── test_client_and_enterprise.py # RateLimiter, Prometheus, snapshots & CPA
├── pyproject.toml            # Build metadata and tool configuration
├── requirements.txt          # Development dependencies (pytest, uvicorn)
├── LICENSE                   # Apache-2.0 Open Source License
└── README.md                 # System specification and documentation
```

---

## Installation & Quickstart

### Prerequisites
* Python 3.11, 3.12, or 3.13
* Virtual environment (recommended)

```bash
git clone https://github.com/Huighlet/GeoMesh-Stream.git
cd GeoMesh-Stream

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Or on Windows: .venv\Scripts\activate

# Install in editable mode
pip install -e .
```

### Running the Test Suite
```bash
pytest -v
```
Output:
```
tests/test_client_and_enterprise.py::test_prometheus_metrics_endpoint PASSED [  3%]
tests/test_client_and_enterprise.py::test_rate_limiter_headers PASSED    [  6%]
tests/test_client_and_enterprise.py::test_geofences_export_manifest PASSED [  9%]
tests/test_client_and_enterprise.py::test_fleet_state_snapshot PASSED    [ 12%]
tests/test_client_and_enterprise.py::test_client_sdk_end_to_end PASSED   [ 16%]
tests/test_client_and_enterprise.py::test_maritime_collision_risk_cpa PASSED [ 19%]
tests/test_engine.py::test_engine_telemetry_and_alerts PASSED            [ 22%]
tests/test_engine.py::test_engine_query_radius PASSED                    [ 25%]
tests/test_geofence.py::test_radial_geofence_containment PASSED          [ 29%]
tests/test_geofence.py::test_polygon_geofence_containment PASSED         [ 32%]
tests/test_geofence.py::test_geofence_engine_transition_lifecycle PASSED [ 35%]
tests/test_geofence.py::test_duplicate_geofence_registration PASSED      [ 38%]
tests/test_index.py::test_spatial_grid_insert_and_query PASSED           [ 41%]
tests/test_index.py::test_spatial_grid_remove PASSED                     [ 45%]
tests/test_index.py::test_spatial_grid_radius_query PASSED               [ 48%]
tests/test_kinematics.py::test_kinematic_velocity_and_bearing PASSED     [ 51%]
tests/test_kinematics.py::test_dead_reckoning_prediction PASSED          [ 54%]
tests/test_math.py::test_haversine_known_distances PASSED                [ 58%]
tests/test_math.py::test_initial_bearing PASSED                          [ 61%]
tests/test_math.py::test_dead_reckoning_destination PASSED               [ 64%]
tests/test_math.py::test_point_in_polygon_convex PASSED                  [ 67%]
tests/test_math.py::test_point_in_polygon_concave PASSED                 [ 70%]
tests/test_server.py::test_health_endpoint PASSED                        [ 74%]
tests/test_server.py::test_leaflet_dashboard_html PASSED                 [ 77%]
tests/test_server.py::test_create_radial_and_polygon_geofence PASSED     [ 80%]
tests/test_server.py::test_telemetry_ingestion_and_proximity PASSED      [ 83%]
tests/test_telecom.py::test_one_time_subscriber_registration PASSED      [ 87%]
tests/test_telecom.py::test_timing_advance_position_derivation PASSED    [ 90%]
tests/test_telecom.py::test_passive_handover_zero_touch_lifecycle PASSED [ 93%]
tests/test_telecom.py::test_multilingual_voice_alert_scripts PASSED      [ 96%]
tests/test_telecom.py::test_telecom_rest_endpoints PASSED                [100%]

======================== 31 passed in 1.89s ========================
```

### Running Standalone Fleet Server & Leaflet.js Dashboard

Launch the turnkey streaming server and open the live map dashboard in your browser:

```bash
geomesh serve --host 0.0.0.0 --port 8080
```

* **Interactive Live Map:** `http://localhost:8080/`
* **Swagger OpenAPI Docs:** `http://localhost:8080/docs`

#### REST API Ingestion Examples:

```bash
# 1. One-Time Registration for Keypad Phone Canoe Fisherman
curl -X POST http://localhost:8008/v1/telecom/register \
  -H "Content-Type: application/json" \
  -d '{
    "phone_number": "+233244111222",
    "owner_name": "Kofi Nyamekye",
    "canoe_name": "Nyame Nti",
    "landing_beach": "Keta Beach",
    "language": "ewe"
  }'

# 2. Passive Cell Handover & Timing Advance Ingestion (Tripwire: At Sea)
curl -X POST http://localhost:8008/v1/telecom/cell-event \
  -H "Content-Type: application/json" \
  -d '{
    "phone_number": "+233244111222",
    "sector_id": "KETA-CSP-S1",
    "timing_advance": 12,
    "signal_dbm": -82
  }'

# 3. Ingest AIS Class-A Telemetry Ping (Crude Oil Tanker calling Tema)
curl -X POST http://localhost:8008/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{
    "entity_id": "MMSI-636019842",
    "lat": 5.7600,
    "lon": 0.9850,
    "speed_mps": 8.2,
    "heading_deg": 245.0
  }'

# 4. Closest Point of Approach (CPA/TCPA) Collision Calculation
curl "http://localhost:8008/v1/maritime/cpa?vessel1_id=MMSI-636019842&vessel2_id=canoe-233244111222"

# 5. Dispatch Multilingual IVR Robocall Alert (Ewe / Ga / Fante)
curl -X POST http://localhost:8008/v1/telecom/alert/voice \
  -H "Content-Type: application/json" \
  -d '{
    "phone_number": "+233244111222",
    "threat_vessel": "Crude Oil Tanker Atlantic Pioneer",
    "cpa_nm": 0.38,
    "tcpa_min": 4.2
  }'
```

### Production Docker Deployment

Deploy with Docker in a single command:

```bash
docker build -t geomesh-stream .
docker run -d -p 8080:8080 --name geomesh geomesh-stream
```

### Executing Microsecond Benchmarks
```bash
geomesh bench -n 100000
```

---

## Python API Usage

```python
from geomesh import GeoMeshEngine, RadialGeofence, PolygonGeofence, Point, TelemetryPing
from geomesh.telecom import TelecomPassiveGateway, MaritimeVoiceAlertService

# 1. Initialize Maritime Geospatial Engine
engine = GeoMeshEngine(cell_size_deg=0.02)
telecom_gw = TelecomPassiveGateway(engine)
voice_service = MaritimeVoiceAlertService()

# 2. One-Time Registration for Keypad Phone Fishermen (Zero Daily Dialing)
sub = telecom_gw.register_subscriber(
    phone_number="+233244111222",
    owner_name="Kofi Nyamekye",
    canoe_name="Nyame Nti",
    landing_beach="Keta Landing Beach",
    language="ewe"
)

# 3. Passive Cell Handover & Timing Advance Coordinate Derivation
# Handset negotiates with Cape St. Paul Seaward Tower (TA=14 -> 7.7 km offshore)
lat, lon = telecom_gw.process_cell_event(
    phone_number="+233244111222",
    sector_id="KETA-CSP-S1",
    timing_advance=14,
    signal_dbm=-82
)
print(f"Passive Canoe Position: lat={lat:.4f}, lon={lon:.4f}")

# 4. Ingest Converging AIS Commercial Tanker
engine.ingest_telemetry(TelemetryPing.create(
    entity_id="MMSI-636019842",
    lat=5.7500,
    lon=0.9900,
    speed_mps=8.5,
    heading_deg=240.0
))

# 5. Calculate Closest Point of Approach (CPA) & Trigger Mother-Tongue Robocall
cpa = engine.get_vessel_cpa("MMSI-636019842", sub.entity_id)
if cpa and cpa["collision_risk"]:
    call = voice_service.dispatch_alert_call(
        subscriber=sub,
        threat_vessel="Crude Oil Tanker Atlantic Pioneer",
        cpa_nm=cpa["cpa_nm"],
        tcpa_min=cpa["tcpa_minutes"]
    )
    print(f"[EMERGENCY CALL DISPATCHED] Language: {call['language']}")
    print(f"Audio Script: {call['voice_script']}")
```

---

## License

This project is licensed under the Apache 2.0 Open Source License. See [LICENSE](LICENSE) for details.
