"""
GeoMesh-Stream: Geospatial Core Types, Coordinates, Bounding Boxes, and Event Models.
"""

from __future__ import annotations

import dataclasses
import enum
import math
import time
from typing import Any, Dict, List, Optional


@dataclasses.dataclass(slots=True, frozen=True)
class Point:
    """
    Geographic coordinate point on the WGS-84 reference ellipsoid.
    lat: Latitude in decimal degrees [-90.0, 90.0]
    lon: Longitude in decimal degrees [-180.0, 180.0]
    """
    lat: float
    lon: float

    def __post_init__(self) -> None:
        if not (-90.0 <= self.lat <= 90.0):
            raise ValueError(f"Latitude must be between -90 and 90 degrees, got {self.lat}")
        if not (-180.0 <= self.lon <= 180.0):
            raise ValueError(f"Longitude must be between -180 and 180 degrees, got {self.lon}")

    def to_tuple(self) -> tuple[float, float]:
        return (self.lat, self.lon)


@dataclasses.dataclass(slots=True, frozen=True)
class BoundingBox:
    """Axis-Aligned Bounding Box (AABB) in geographical coordinate space."""
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

    def contains(self, point: Point) -> bool:
        """Fast bounding box containment check."""
        return (
            self.min_lat <= point.lat <= self.max_lat
            and self.min_lon <= point.lon <= self.max_lon
        )

    def intersects(self, other: BoundingBox) -> bool:
        """Check if two bounding boxes overlap."""
        return not (
            self.max_lat < other.min_lat
            or self.min_lat > other.max_lat
            or self.max_lon < other.min_lon
            or self.min_lon > other.max_lon
        )


class GeofenceType(enum.StrEnum):
    """Shape type of the geofence perimeter."""
    RADIAL = "RADIAL"
    POLYGON = "POLYGON"


class GeofenceEvent(enum.StrEnum):
    """Geofence boundary breach transition event."""
    ENTER = "ENTER"
    EXIT = "EXIT"
    DWELL = "DWELL"


@dataclasses.dataclass(slots=True, frozen=True)
class TelemetryPing:
    """
    Incoming GPS telemetry ping from an asset or sensor.
    """
    entity_id: str
    point: Point
    timestamp_ns: int
    speed_mps: Optional[float] = None
    heading_deg: Optional[float] = None
    altitude_m: Optional[float] = None
    accuracy_m: Optional[float] = None

    @classmethod
    def create(
        cls,
        entity_id: str,
        lat: float,
        lon: float,
        speed_mps: Optional[float] = None,
        heading_deg: Optional[float] = None,
        timestamp_ns: Optional[int] = None,
    ) -> TelemetryPing:
        ts = timestamp_ns if timestamp_ns is not None else time.time_ns()
        return cls(
            entity_id=entity_id,
            point=Point(lat=lat, lon=lon),
            timestamp_ns=ts,
            speed_mps=speed_mps,
            heading_deg=heading_deg,
        )


@dataclasses.dataclass(slots=True, frozen=True)
class GeofenceAlert:
    """Emitted alert when an entity breaches or transitions across a geofence."""
    alert_id: str
    entity_id: str
    geofence_id: str
    event: GeofenceEvent
    timestamp_ns: int
    point: Point
    dwell_duration_sec: float = 0.0


@dataclasses.dataclass(slots=True)
class KinematicState:
    """Tracked velocity, bearing, and motion profile of an active entity."""
    entity_id: str
    last_point: Point
    last_timestamp_ns: int
    velocity_mps: float = 0.0
    bearing_deg: float = 0.0
    acceleration_mps2: float = 0.0


# -------------------------------------------------------------------------
# Exceptions
# -------------------------------------------------------------------------

class GeoMeshError(Exception):
    """Base exception for all GeoMesh-Stream operations."""


class GeofenceAlreadyExistsError(GeoMeshError):
    """Raised when registering a geofence with a duplicated ID."""


class GeofenceNotFoundError(GeoMeshError):
    """Raised when referencing an unknown geofence ID."""


class InvalidPolygonError(GeoMeshError):
    """Raised when polygon vertices do not form a valid closed polygon."""
