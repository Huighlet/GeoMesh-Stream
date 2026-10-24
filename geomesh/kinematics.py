"""
GeoMesh-Stream: Kinematic Telemetry Tracking & Dead-Reckoning Extrapolation.
Derives velocity, bearing, acceleration, and projects future coordinates during GPS packet drop.
"""

from __future__ import annotations

from typing import Dict, Optional

from geomesh.math import dead_reckoning_destination, haversine_distance, initial_bearing
from geomesh.types import KinematicState, Point, TelemetryPing


class KinematicTracker:
    """
    Real-time kinematic state estimator for moving assets.
    Maintains velocity vectors, heading azimuths, and computes dead-reckoning projections.
    """

    def __init__(self, max_plausible_speed_mps: float = 350.0) -> None:
        # 350 m/s (~1260 km/h) covers high-speed ground vehicles and commercial aviation
        self.max_plausible_speed_mps = max_plausible_speed_mps
        self._states: Dict[str, KinematicState] = {}

    def get_state(self, entity_id: str) -> Optional[KinematicState]:
        """Retrieve current kinematic state of an entity."""
        return self._states.get(entity_id)

    def update(self, ping: TelemetryPing) -> KinematicState:
        """
        Ingest a telemetry ping and update the entity's kinematic vector.
        """
        prev = self._states.get(ping.entity_id)
        if not prev:
            init_v = ping.speed_mps if ping.speed_mps is not None else 0.0
            init_b = ping.heading_deg if ping.heading_deg is not None else 0.0
            state = KinematicState(
                entity_id=ping.entity_id,
                last_point=ping.point,
                last_timestamp_ns=ping.timestamp_ns,
                velocity_mps=init_v,
                bearing_deg=init_b,
                acceleration_mps2=0.0,
            )
            self._states[ping.entity_id] = state
            return state

        # Calculate time delta in seconds
        dt = (ping.timestamp_ns - prev.last_timestamp_ns) / 1_000_000_000.0
        if dt <= 0.0:
            # Out-of-order or duplicate timestamp, retain previous velocity
            return prev

        dist_m = haversine_distance(prev.last_point, ping.point)
        calc_v = dist_m / dt

        # Sanity check against GPS teleportation/spoofing
        if calc_v > self.max_plausible_speed_mps and ping.speed_mps is None:
            # Reject anomalous jump, preserve previous state
            return prev

        v = ping.speed_mps if ping.speed_mps is not None else calc_v
        
        # Calculate bearing if significant displacement (>0.5m)
        if dist_m > 0.5:
            b = initial_bearing(prev.last_point, ping.point)
        elif ping.heading_deg is not None:
            b = ping.heading_deg
        else:
            b = prev.bearing_deg

        # Calculate acceleration
        acc = (v - prev.velocity_mps) / dt

        new_state = KinematicState(
            entity_id=ping.entity_id,
            last_point=ping.point,
            last_timestamp_ns=ping.timestamp_ns,
            velocity_mps=v,
            bearing_deg=b,
            acceleration_mps2=acc,
        )
        self._states[ping.entity_id] = new_state
        return new_state

    def predict_position(self, entity_id: str, future_delta_sec: float) -> Optional[Point]:
        """
        Extrapolate entity position using Dead-Reckoning:
        distance = v * dt + 0.5 * a * (dt^2)
        """
        state = self._states.get(entity_id)
        if not state or future_delta_sec <= 0.0:
            return state.last_point if state else None

        # Project distance (clamped to prevent negative distance if decelerating)
        dt = future_delta_sec
        dist = (state.velocity_mps * dt) + (0.5 * state.acceleration_mps2 * (dt * dt))
        dist = max(0.0, dist)

        return dead_reckoning_destination(
            p=state.last_point,
            distance_m=dist,
            bearing_deg=state.bearing_deg,
        )
