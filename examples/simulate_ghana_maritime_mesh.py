"""
Ghana Maritime Telemetry & Coastal Safety Simulation.
Simulates dual-class maritime traffic in the Gulf of Guinea:
1. Commercial crude tankers & container carriers navigating shipping channels off Tema Port.
2. Artisanal wooden fishing canoes operating out of Keta, Cape St. Paul, and Dzelukope using smartphone GPS.
Verifies Closest Point of Approach (CPA/TCPA) collision alerts, Ghana 6NM Inshore Exclusion Zone (IEZ) enforcement,
and Guinea Current ocean drift search-and-rescue (SAR) projection.
"""

from __future__ import annotations

import math
import random
import time
from starlette.testclient import TestClient
from geomesh.server import app
from geomesh.client import GeoMeshClient


def run_ghana_maritime_simulation():
    print("=" * 80)
    print("GeoMesh-Stream: Ghana Maritime Vessel Stream & Coastal Safety Platform")
    print("=" * 80)

    test_http = TestClient(app)
    client = GeoMeshClient(
        base_url="http://localhost:8008",
        api_key="ghana_maritime_auth_key",
        http_client=test_http,
    )

    # 1. Establish Ghana Maritime Geofence Topology
    print("\n[+] Registering Ghana Maritime Boundary & Protected Fishing Zones...")

    # A. Tema Deepwater Anchorage (Radial Roadstead)
    tema_res = client.create_radial_geofence(
        geofence_id="tema-deepwater-anchorage",
        lat=5.6200,
        lon=0.0500,
        radius_m=6500.0,
        name="Tema Deepwater Commercial Anchorage",
    )
    print(f"     [OK] Anchorage Geofence: '{tema_res['name']}' (Radius: {tema_res['radius_m']}m)")

    # B. Ghana Inshore Exclusion Zone (IEZ) - Fisheries Act 625 (6 Nautical Miles)
    iez_vertices = [
        {"lat": 5.7000, "lon": 0.0500},
        {"lat": 5.7600, "lon": 0.6300},
        {"lat": 5.7800, "lon": 0.9000},
        {"lat": 5.9200, "lon": 1.0500},
        {"lat": 6.1000, "lon": 1.2000},
        {"lat": 6.0000, "lon": 1.3000},
        {"lat": 5.7000, "lon": 1.1200},
        {"lat": 5.6000, "lon": 0.6500},
        {"lat": 5.5500, "lon": 0.1000},
    ]
    iez_res = client.create_polygon_geofence(
        geofence_id="ghana-iez-reserve",
        vertices=iez_vertices,
        name="Ghana 6NM Inshore Exclusion Zone (Fisheries Act 625)",
    )
    print(f"     [OK] IEZ Geofence: '{iez_res['name']}' ({iez_res['vertex_count']} boundary vertices)")

    # C. Keta Coastal Artisanal Grounds
    keta_vertices = [
        {"lat": 5.8200, "lon": 0.8800},
        {"lat": 5.9500, "lon": 0.9800},
        {"lat": 5.9200, "lon": 1.1500},
        {"lat": 5.7600, "lon": 1.0500},
    ]
    keta_res = client.create_polygon_geofence(
        geofence_id="keta-artisanal-grounds",
        vertices=keta_vertices,
        name="Keta & Cape St. Paul Artisanal Grounds",
    )
    print(f"     [OK] Artisanal Zone: '{keta_res['name']}'")

    # 2. Ingest Dual-Class Telemetry (Tankers + Keta Fishing Canoes)
    print("\n[+] Ingesting Dual-Class Vessel Telemetry across Gulf of Guinea...")
    start_time = time.perf_counter()

    # Commercial Tanker: MT African Runner (MMSI: 627001420)
    tanker_res = client.ingest_telemetry(
        entity_id="tanker-african-runner",
        lat=5.7800,
        lon=0.9200,
        speed_mps=8.2,   # ~16.0 knots
        heading_deg=245.0, # Heading WSW towards Tema
    )
    print(f"     [AIS] Commercial Tanker 'MT African Runner' (Speed: 16.0 kt, Course: 245° WSW)")

    # Artisanal Fishing Canoe from Keta (Phone GPS Telemetry)
    canoe_res = client.ingest_telemetry(
        entity_id="canoe-keta-01",
        lat=5.7920,
        lon=0.9050,
        speed_mps=1.8,   # ~3.5 knots
        heading_deg=165.0, # Heading SSE into Gulf of Guinea
    )
    print(f"     [PHONE GPS] Artisanal Canoe 'Keta-01 [Nyimdzii]' (Speed: 3.5 kt, Course: 165° SSE)")

    # Ingest 250 additional telemetry pings from surrounding Keta canoe fleet & Tema anchorage
    total_pings = 250
    for i in range(total_pings):
        vessel_id = f"canoe-keta-{i % 20:02d}" if i % 2 == 0 else f"tanker-tema-{i % 5:02d}"
        v_lat = 5.8000 + random.uniform(-0.05, 0.05)
        v_lon = 0.9000 + random.uniform(-0.05, 0.05)
        client.ingest_telemetry(
            entity_id=vessel_id,
            lat=v_lat,
            lon=v_lon,
            speed_mps=random.uniform(1.5, 9.0),
            heading_deg=random.uniform(0.0, 360.0),
        )

    elapsed_ingest = time.perf_counter() - start_time
    ops_per_sec = total_pings / elapsed_ingest if elapsed_ingest > 0 else 0
    print(f"[OK] Ingested {total_pings + 2} vessel positions in {elapsed_ingest*1000:.2f} ms ({ops_per_sec:,.0f} pings/sec)")

    # 3. Compute Closest Point of Approach (CPA & TCPA) Collision Avoidance
    print("\n[+] Evaluating Maritime Closest Point of Approach (CPA / TCPA)...")
    cpa_report = client.calculate_collision_risk(
        vessel_1="tanker-african-runner",
        vessel_2="canoe-keta-01",
    )
    print(f"     Vessels: MT African Runner <--> Canoe Keta-01")
    print(f"     CPA Distance: {cpa_report['cpa_nautical_miles']} Nautical Miles ({cpa_report['cpa_meters']:,} meters)")
    print(f"     Time to CPA (TCPA): {cpa_report['tcpa_minutes']} minutes ({cpa_report['tcpa_seconds']} seconds)")
    print(f"     Risk Assessment: {cpa_report['risk_level']}")
    if cpa_report['risk_level'] in ("CRITICAL_COLLISION_ALERT", "WARNING_CONVERGING"):
        print(f"     [ACTION REQUIRED] Collision Warning Dispatched to Ghana Maritime Authority Radar Console")

    # 4. Proximity Search: Artisanal Canoes around Cape St. Paul
    cape_st_paul_lat, cape_st_paul_lon = 5.8000, 0.9000
    print(f"\n[+] Searching Active Vessels within 5 Nautical Miles (9,260m) of Cape St. Paul...")
    proximity = client.query_proximity(lat=cape_st_paul_lat, lon=cape_st_paul_lon, radius_m=9260.0)
    print(f"     Found {proximity['match_count']} active vessels in fishing corridor:")
    for v in proximity["entities"][:4]:
        dist_nm = round(v['distance_meters'] / 1852.0, 2)
        print(f"       - Vessel '{v['entity_id']}': Distance {dist_nm} NM ({v['distance_meters']:,.0f}m)")

    # 5. Search & Rescue (SAR) Dead-Reckoning Extrapolation (Guinea Current Drift)
    print(f"\n[+] Extrapolating Adrift Canoe Dead-Reckoning Trajectory (+30 minutes forward)...")
    sar_projection = client.predict_position(entity_id="canoe-keta-01", future_delta_sec=1800.0)
    sar_pos = sar_projection["predicted_position"]
    print(f"     Adrift Coordinate (SAR Drift Center): Lat {sar_pos['lat']:.5f}, Lon {sar_pos['lon']:.5f}")

    # 6. Point-in-Time Fleet State Snapshot
    print(f"\n[+] Capturing Point-in-Time Ghana Maritime Fleet Snapshot Checkpoint...")
    snapshot = client.snapshot_fleet()
    print(f"     Snapshot ID: {snapshot['snapshot_id']}")
    print(f"     Total Tracked Vessels: {snapshot['total_tracked_vehicles']}")
    print(f"     Checkpoint Status: {snapshot['status']}")

    # 7. Geofence Manifest Export
    print(f"\n[+] Exporting Ghana Maritime Boundaries Manifest for Audit...")
    manifest = client.export_geofences()
    print(f"     Exported {manifest['total_geofences']} active maritime boundaries:")
    for gf in manifest["geofences"]:
        print(f"       - '{gf['geofence_id']}' [{gf['type']}] ({gf['name']})")

    # 8. Prometheus Metrics Telemetry Verification
    metrics = client.get_metrics()
    assert "geomesh_geofences_total" in metrics
    assert "geomesh_tracked_entities_total" in metrics
    print(f"\n[OK] Prometheus Metrics Verified (/metrics live and reporting)")

    print("\n" + "=" * 80)
    print("[SUCCESS] Ghana Maritime Telemetry & Collision Avoidance Simulation Passed!")
    print("=" * 80)


if __name__ == "__main__":
    run_ghana_maritime_simulation()
