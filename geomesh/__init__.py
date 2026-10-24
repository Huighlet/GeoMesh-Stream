"""
GeoMesh-Stream: Real-Time Ghana Maritime Vessel Stream, Collision Avoidance & Coastal Geofence Engine.
"""

from geomesh.ais import AISDecoder, AISPositionReport, AISStaticReport
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
from geomesh.sar import GuineaCurrentSARModel, SARWatchdog
from geomesh.storage import (
    DurableStore,
    SARIncidentRecord,
    SubscriberRecord,
    VoiceCallRecord,
    VoyageRecord,
)
from geomesh.telecom import (
    CanoeSubscriber,
    Language,
    MaritimeVoiceAlertService,
    TelecomPassiveGateway,
    TowerSector,
    VesselStatus,
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
    "AISDecoder",
    "AISPositionReport",
    "AISStaticReport",
    "BoundingBox",
    "CanoeSubscriber",
    "DurableStore",
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
    "GuineaCurrentSARModel",
    "InvalidPolygonError",
    "KinematicState",
    "KinematicTracker",
    "Language",
    "MaritimeVoiceAlertService",
    "Point",
    "PolygonGeofence",
    "RadialGeofence",
    "SARIncidentRecord",
    "SARWatchdog",
    "SpatialGridIndex",
    "SubscriberRecord",
    "TelecomPassiveGateway",
    "TelemetryPing",
    "TowerSector",
    "VesselStatus",
    "VoiceCallRecord",
    "VoyageRecord",
    "dead_reckoning_destination",
    "haversine_distance",
    "initial_bearing",
    "point_in_polygon",
]
