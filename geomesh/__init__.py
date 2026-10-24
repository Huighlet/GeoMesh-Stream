"""
GeoMesh-Stream: Real-Time Telemetry & Spatial Geofence Stream Engine.
"""

from geomesh.engine import GeoMeshEngine
from geomesh.geofence import (
    Geofence,
    GeofenceEngine,
    PolygonGeofence,
    RadialGeofence,
)
from geomesh.index import SpatialGridIndex
from geomesh.kinematics import KinematicTracker
from geomesh.math import (
    EARTH_RADIUS_METERS,
    dead_reckoning_destination,
    haversine_distance,
    initial_bearing,
    point_in_polygon,
)
from geomesh.types import (
    BoundingBox,
    GeofenceAlert,
    GeofenceAlreadyExistsError,
    GeofenceEvent,
    GeofenceNotFoundError,
    GeofenceType,
    GeoMeshError,
    InvalidPolygonError,
    KinematicState,
    Point,
    TelemetryPing,
)

__version__ = "1.0.0"

__all__ = [
    "BoundingBox",
    "EARTH_RADIUS_METERS",
    "Geofence",
    "GeofenceAlert",
    "GeofenceAlreadyExistsError",
    "GeofenceEngine",
    "GeofenceEvent",
    "GeofenceNotFoundError",
    "GeofenceType",
    "GeoMeshEngine",
    "GeoMeshError",
    "InvalidPolygonError",
    "KinematicState",
    "KinematicTracker",
    "Point",
    "PolygonGeofence",
    "RadialGeofence",
    "SpatialGridIndex",
    "TelemetryPing",
    "dead_reckoning_destination",
    "haversine_distance",
    "initial_bearing",
    "point_in_polygon",
]
