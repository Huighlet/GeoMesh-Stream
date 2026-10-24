"""
GeoMesh-Stream: Command Line Interface & Microsecond Geospatial Benchmark Suite.
"""

from __future__ import annotations

import argparse
import sys
import time

from geomesh.engine import GeoMeshEngine
from geomesh.geofence import PolygonGeofence, RadialGeofence
from geomesh.index import SpatialGridIndex
from geomesh.kinematics import KinematicTracker
from geomesh.math import (
    dead_reckoning_destination,
    haversine_distance,
    initial_bearing,
    point_in_polygon,
)
from geomesh.types import BoundingBox, Point, TelemetryPing


def run_benchmark(iterations: int = 100000) -> None:
    """Run comprehensive microsecond benchmarks across all core geospatial components."""
    print("=" * 76)
    print("GeoMesh-Stream: Microsecond Geospatial Throughput Benchmark Suite")
    print(f"Iterations: {iterations:,} operations per test")
    print("=" * 76)

    # 1. Haversine Great-Circle Distance
    p1 = Point(lat=5.6037, lon=-0.1870)  # Accra, Ghana
    p2 = Point(lat=5.6148, lon=-0.1731)  # Airport Residential, Accra

    t0 = time.perf_counter()
    for _ in range(iterations):
        haversine_distance(p1, p2)
    t1 = time.perf_counter()
    dur_hav = t1 - t0
    ops_hav = iterations / dur_hav
    lat_hav_us = (dur_hav / iterations) * 1_000_000

    print(f"[1] Haversine Great-Circle Distance:")
    print(f"    Throughput: {ops_hav:12,.1f} calculations/sec")
    print(f"    Latency:    {lat_hav_us:12.3f} us/calc\n")

    # 2. Initial Forward Azimuth / Bearing
    t0 = time.perf_counter()
    for _ in range(iterations):
        initial_bearing(p1, p2)
    t1 = time.perf_counter()
    dur_bearing = t1 - t0
    ops_bearing = iterations / dur_bearing
    lat_bearing_us = (dur_bearing / iterations) * 1_000_000

    print(f"[2] Initial Bearing / Heading Forward Azimuth:")
    print(f"    Throughput: {ops_bearing:12,.1f} calculations/sec")
    print(f"    Latency:    {lat_bearing_us:12.3f} us/calc\n")

    # 3. Ray-Casting Point-in-Polygon (5-Vertex Polygon)
    polygon_vertices = [
        Point(lat=5.6000, lon=-0.2000),
        Point(lat=5.6200, lon=-0.2000),
        Point(lat=5.6300, lon=-0.1800),
        Point(lat=5.6100, lon=-0.1700),
        Point(lat=5.5900, lon=-0.1900),
    ]
    test_pt = Point(lat=5.6100, lon=-0.1850)

    t0 = time.perf_counter()
    for _ in range(iterations):
        point_in_polygon(test_pt, polygon_vertices)
    t1 = time.perf_counter()
    dur_pip = t1 - t0
    ops_pip = iterations / dur_pip
    lat_pip_us = (dur_pip / iterations) * 1_000_000

    print(f"[3] Ray-Casting Point-in-Polygon (5 Vertices):")
    print(f"    Throughput: {ops_pip:12,.1f} checks/sec")
    print(f"    Latency:    {lat_pip_us:12.3f} us/check\n")

    # 4. Spatial Grid Index Point Lookup (with 500 Indexed Geofences)
    grid = SpatialGridIndex(cell_size_deg=0.02)
    for i in range(500):
        # Distribute fences around Accra
        c_lat = 5.5 + (i * 0.001)
        c_lon = -0.3 + (i * 0.001)
        bbox = BoundingBox(c_lat, c_lon, c_lat + 0.01, c_lon + 0.01)
        grid.insert(f"fence-{i}", bbox)

    t0 = time.perf_counter()
    for _ in range(iterations):
        grid.query_point(test_pt)
    t1 = time.perf_counter()
    dur_grid = t1 - t0
    ops_grid = iterations / dur_grid
    lat_grid_us = (dur_grid / iterations) * 1_000_000

    print(f"[4] Spatial Grid Index O(1) Candidate Query (500 Geofences):")
    print(f"    Throughput: {ops_grid:12,.1f} queries/sec")
    print(f"    Latency:    {lat_grid_us:12.3f} us/query\n")

    # 5. Kinematic Dead-Reckoning Extrapolation
    tracker = KinematicTracker()
    p0 = TelemetryPing.create("vehicle-101", lat=5.6000, lon=-0.1800, speed_mps=25.0, heading_deg=45.0, timestamp_ns=1_000_000_000)
    tracker.update(p0)

    t0 = time.perf_counter()
    for _ in range(iterations):
        tracker.predict_position("vehicle-101", future_delta_sec=10.0)
    t1 = time.perf_counter()
    dur_dr = t1 - t0
    ops_dr = iterations / dur_dr
    lat_dr_us = (dur_dr / iterations) * 1_000_000

    print(f"[5] Kinematic Dead-Reckoning Position Projection:")
    print(f"    Throughput: {ops_dr:12,.1f} projections/sec")
    print(f"    Latency:    {lat_dr_us:12.3f} us/proj\n")

    # 6. Full Engine Telemetry Ingestion & Geofence Breach Pipeline
    engine = GeoMeshEngine(cell_size_deg=0.02)
    # Register mix of radial and polygon geofences
    engine.register_geofence(RadialGeofence("airport-zone", center=p1, radius_m=2000.0))
    engine.register_geofence(PolygonGeofence("port-zone", vertices=polygon_vertices))

    telemetry_stream = [
        TelemetryPing.create(
            entity_id=f"asset-{i % 100}",
            lat=5.6000 + (i * 0.0001),
            lon=-0.1870 + (i * 0.0001),
            speed_mps=15.0,
            timestamp_ns=1_000_000_000 + (i * 1_000_000_000),
        )
        for i in range(min(iterations, 25000))
    ]

    t0 = time.perf_counter()
    alert_count = 0
    for ping in telemetry_stream:
        alerts = engine.ingest_telemetry(ping)
        alert_count += len(alerts)
    t1 = time.perf_counter()
    dur_eng = t1 - t0
    ops_eng = len(telemetry_stream) / dur_eng
    lat_eng_us = (dur_eng / len(telemetry_stream)) * 1_000_000

    print(f"[6] Full Engine Ingestion & Geofence Breach Pipeline:")
    print(f"    Throughput: {ops_eng:12,.1f} pings/sec")
    print(f"    Latency:    {lat_eng_us:12.3f} us/ping")
    print(f"    Breach Alerts Emitted: {alert_count:,}")
    print("=" * 76)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="geomesh",
        description="GeoMesh-Stream: Real-Time Telemetry & Spatial Geofence Stream Engine",
    )
    subparsers = parser.add_subparsers(dest="command")

    bench_parser = subparsers.add_parser("bench", help="Run geospatial performance benchmark suite")
    bench_parser.add_argument("-n", "--iterations", type=int, default=100000, help="Number of iterations")

    args = parser.parse_args()
    if args.command == "bench":
        run_benchmark(iterations=args.iterations)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
