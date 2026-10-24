"""
Enterprise Simulation: Real-Time Urban Delivery Fleet & Geospatial Mesh Engine.
Simulates 50 delivery vans streaming high-frequency GPS telemetry across dynamic radial and polygon geofences,
evaluating perimeter breaches, proximity dispatching, kinematic dead reckoning, and durable fleet snapshots.
"""

from __future__ import annotations

import math
import random
import time
from starlette.testclient import TestClient
from geomesh.server import app
from geomesh.client import GeoMeshClient


def run_fleet_simulation():
    print("=" * 75)
    print("GeoMesh-Stream Enterprise Simulation: Real-Time Fleet & Geofence Mesh")
    print("=" * 75)

    test_http = TestClient(app)
    client = GeoMeshClient(
        base_url="http://localhost:8000",
        api_key="geomesh_enterprise_secret_key",
        http_client=test_http,
    )

    # 1. Establish Spatial Geofence Perimeters
    print("\n[+] Registering Enterprise Geofence Topology...")
    
    # Hub: Circular perimeter
    hub_res = client.create_radial_geofence(
        geofence_id="central-distribution-hub",
        lat=5.6050,
        lon=-0.1700,
        radius_m=1500.0,
        name="Airport Central Logistics Hub",
    )
    print(f"     [OK] Radial Geofence: '{hub_res['name']}' (Radius: {hub_res['radius_m']}m)")

    # Congestion Zone: Polygon perimeter
    polygon_vertices = [
        {"lat": 5.5900, "lon": -0.2000},
        {"lat": 5.6100, "lon": -0.2000},
        {"lat": 5.6100, "lon": -0.1800},
        {"lat": 5.5900, "lon": -0.1800},
    ]
    poly_res = client.create_polygon_geofence(
        geofence_id="downtown-congestion-zone",
        vertices=polygon_vertices,
        name="Downtown Low Emission Zone",
    )
    print(f"     [OK] Polygon Geofence: '{poly_res['name']}' ({poly_res['vertex_count']} boundary vertices)")

    # 2. Simulate 50 Delivery Vans Streaming Telemetry
    total_vans = 50
    total_pings = 300
    print(f"\n[+] Ingesting {total_pings} GPS telemetry pings across {total_vans} delivery vans...")

    start_time = time.perf_counter()
    alerts_recorded = []
    
    for i in range(total_pings):
        van_id = f"delivery-van-{i % total_vans:03d}"
        
        # Base coordinate with motion trajectory moving towards hub
        step_factor = (i // total_vans) * 0.002
        lat = 5.5850 + step_factor + random.uniform(-0.001, 0.001)
        lon = -0.1850 + step_factor + random.uniform(-0.001, 0.001)
        speed = round(random.uniform(8.0, 22.0), 1)  # m/s (~30 - 80 km/h)
        heading = round(random.uniform(30.0, 60.0), 1)

        res = client.ingest_telemetry(
            entity_id=van_id,
            lat=lat,
            lon=lon,
            speed_mps=speed,
            heading_deg=heading,
        )

        if res.get("alerts"):
            for alert in res["alerts"]:
                alerts_recorded.append((van_id, alert))

    elapsed_ingest = time.perf_counter() - start_time
    ops_per_sec = total_pings / elapsed_ingest if elapsed_ingest > 0 else 0

    print(f"[OK] Ingested {total_pings} telemetry pings in {elapsed_ingest*1000:.2f} ms ({ops_per_sec:,.0f} pings/sec)")
    print(f"     Geofence Perimeter Alerts Triggered: {len(alerts_recorded)}")
    for van, alt in alerts_recorded[:5]:
        print(f"       -> [{alt['event']}] Van '{van}' on Geofence '{alt['geofence_id']}' (Dwell: {alt['dwell_duration_sec']}s)")

    # 3. Spatial Proximity Dispatch Query
    hub_lat, hub_lon = 5.6050, -0.1700
    print(f"\n[+] Executing Proximity Search: Vans within 2,500m of Central Hub ({hub_lat}, {hub_lon})...")
    proximity = client.query_proximity(lat=hub_lat, lon=hub_lon, radius_m=2500.0)
    print(f"     Found {proximity['match_count']} active fleet vehicles within radius:")
    for v in proximity["entities"][:4]:
        print(f"       - Van '{v['entity_id']}': Distance {v['distance_meters']:,.0f}m from hub")

    # 4. Kinematic Dead-Reckoning Trajectory Projection
    target_van = "delivery-van-001"
    print(f"\n[+] Extrapolating Dead-Reckoning Trajectory for '{target_van}' (+30 seconds forward)...")
    projection = client.predict_position(entity_id=target_van, future_delta_sec=30.0)
    pred_pos = projection["predicted_position"]
    print(f"     Predicted Coordinates: Lat {pred_pos['lat']:.5f}, Lon {pred_pos['lon']:.5f}")

    # 5. Point-in-Time Fleet Snapshot Durability Checkpoint
    print(f"\n[+] Capturing Point-in-Time Fleet State Snapshot Checkpoint...")
    snapshot = client.snapshot_fleet()
    print(f"     Snapshot ID: {snapshot['snapshot_id']}")
    print(f"     Tracked Fleet Vehicles: {snapshot['total_tracked_vehicles']}")
    print(f"     Durability Status: {snapshot['status']}")

    # 6. Geofence Configuration Audit Export
    print(f"\n[+] Exporting Complete Geofence Manifest for Disaster Recovery...")
    export = client.export_geofences()
    print(f"     Exported {export['total_geofences']} active perimeter zones:")
    for gf in export["geofences"]:
        print(f"       - '{gf['geofence_id']}' [{gf['type']}] ({gf['name']})")

    # 7. Prometheus Metrics Telemetry Verification
    metrics = client.get_metrics()
    assert "geomesh_geofences_total" in metrics
    assert "geomesh_tracked_entities_total" in metrics
    print(f"\n[OK] Prometheus Metrics Verified (/metrics live and reporting)")

    print("\n" + "=" * 75)
    print("[SUCCESS] GeoMesh-Stream Enterprise Delivery Fleet Simulation Passed!")
    print("=" * 75)


if __name__ == "__main__":
    run_fleet_simulation()
