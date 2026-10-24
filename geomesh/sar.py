"""geomesh.sar: Search and Rescue (SAR) Overdue Watchdog & Guinea Current Drift Extrapolator.

Models physical ocean current and wind leeway vectors in the Gulf of Guinea to compute
search datums and generate formal SAR Incident Dockets for the Ghana Maritime Authority (GMA)
and Ghana Navy Eastern Naval Command (Tema).
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from geomesh.math import initial_bearing, dead_reckoning_destination, haversine_distance
from geomesh.storage import DurableStore, SARIncidentRecord, SubscriberRecord, VoyageRecord
from geomesh.types import Point


@dataclass
class DriftVector:
    """Ocean current and wind leeway kinematic components."""
    speed_knots: float
    bearing_deg: float

    @property
    def speed_mps(self) -> float:
        return self.speed_knots * 0.514444


class GuineaCurrentSARModel:
    """Physical trajectory drift model for disabled artisanal canoes in the Gulf of Guinea.

    Incorporates:
    - Eastern Guinea Current: permanent eastward surface stream (~1.4 knots, 090° bearing)
    - Prevailing South-Westerly maritime wind leeway (2.8% leeway coefficient)
    - Cumulative probability search ellipse expansion
    """

    # Gulf of Guinea default oceanographic parameters
    DEFAULT_GUINEA_CURRENT_KNOTS = 1.4  # knots
    DEFAULT_GUINEA_CURRENT_BEARING = 90.0  # degrees (Eastward towards Togo/Benin)
    DEFAULT_WIND_SPEED_KNOTS = 10.0  # knots
    DEFAULT_WIND_DIR_DEG = 225.0  # SW wind blowing towards NE (045°)
    LEEWAY_FACTOR = 0.028  # 2.8% of wind speed for low-freeboard wooden canoes
    INITIAL_POSITION_ERROR_NM = 1.5  # ~2.7 km initial TA cell uncertainty

    @classmethod
    def compute_composite_drift(
        cls,
        current_speed_kt: float = DEFAULT_GUINEA_CURRENT_KNOTS,
        current_dir_deg: float = DEFAULT_GUINEA_CURRENT_BEARING,
        wind_speed_kt: float = DEFAULT_WIND_SPEED_KNOTS,
        wind_dir_deg: float = DEFAULT_WIND_DIR_DEG,
    ) -> DriftVector:
        """Vector addition of ocean current and downwind leeway."""
        # 1. Current vector (u_c, v_c)
        rad_c = math.radians(current_dir_deg)
        u_c = current_speed_kt * math.sin(rad_c)
        v_c = current_speed_kt * math.cos(rad_c)

        # 2. Leeway vector: blowing towards (wind_dir + 180) % 360 or wind downwind
        downwind_deg = (wind_dir_deg + 180.0) % 360.0
        rad_w = math.radians(downwind_deg)
        leeway_speed = wind_speed_kt * cls.LEEWAY_FACTOR
        u_w = leeway_speed * math.sin(rad_w)
        v_w = leeway_speed * math.cos(rad_w)

        # Composite vector
        u_tot = u_c + u_w
        v_tot = v_c + v_w

        tot_speed = math.hypot(u_tot, v_tot)
        tot_bearing = (math.degrees(math.atan2(u_tot, v_tot)) + 360.0) % 360.0

        return DriftVector(speed_knots=tot_speed, bearing_deg=tot_bearing)

    @classmethod
    def extrapolate_datum(
        cls,
        last_lat: float,
        last_lon: float,
        hours_adrift: float,
        drift_vector: Optional[DriftVector] = None,
    ) -> Dict[str, Any]:
        """Extrapolate most probable search datum and expanding search radius."""
        dv = drift_vector or cls.compute_composite_drift()
        hours = max(0.05, hours_adrift)

        # Total drift distance in Nautical Miles and meters
        drift_dist_nm = dv.speed_knots * hours
        drift_dist_m = drift_dist_nm * 1852.0

        # Project Datum Point using Great-Circle Dead-Reckoning
        p0 = Point(lat=last_lat, lon=last_lon)
        p_datum = dead_reckoning_destination(
            p=p0,
            distance_m=drift_dist_m,
            bearing_deg=dv.bearing_deg,
        )

        # Search Radius calculation: R = R_0 + 0.15 * d_drift
        search_radius_nm = cls.INITIAL_POSITION_ERROR_NM + (0.15 * drift_dist_nm)

        # Check international boundary incursion (Togo maritime border at 1.2000° E)
        in_togo_waters = p_datum.lon >= 1.2000

        return {
            "hours_adrift": round(hours, 2),
            "drift_speed_knots": round(dv.speed_knots, 2),
            "drift_bearing_deg": round(dv.bearing_deg, 1),
            "drift_distance_nm": round(drift_dist_nm, 2),
            "datum_lat": round(p_datum.lat, 5),
            "datum_lon": round(p_datum.lon, 5),
            "search_radius_nm": round(search_radius_nm, 2),
            "search_radius_km": round(search_radius_nm * 1.852, 2),
            "togo_border_incursion": in_togo_waters,
        }


class SARWatchdog:
    """Automated operational watchdog tracking overdue voyages and managing SAR dockets."""

    def __init__(self, store: DurableStore):
        self.store = store

    def declare_incident(
        self,
        phone_number: str,
        reason: str = "OVERDUE_VOYAGE_MOTOR_FAILURE",
        custom_hours_adrift: Optional[float] = None,
    ) -> SARIncidentRecord:
        """Create or update an active Search and Rescue incident docket."""
        sub = self.store.get_subscriber(phone_number)
        if not sub:
            raise ValueError(f"Subscriber {phone_number} not found in database.")

        now = time.time()
        hours_adrift = custom_hours_adrift
        if hours_adrift is None:
            if sub.last_seen_at > 0:
                hours_adrift = max(0.5, (now - sub.last_seen_at) / 3600.0)
            else:
                hours_adrift = 6.0

        last_lat = sub.last_lat if sub.last_lat != 0.0 else 5.8200
        last_lon = sub.last_lon if sub.last_lon != 0.0 else 0.8800

        datum = GuineaCurrentSARModel.extrapolate_datum(
            last_lat=last_lat,
            last_lon=last_lon,
            hours_adrift=hours_adrift,
        )

        incident_id = f"SAR-GMA-{sub.canoe_name.replace(' ', '').upper()}-{int(now)}"

        # Generate formal Ghana Maritime Authority (GMA) Emergency Docket
        docket = {
            "incident_id": incident_id,
            "priority": "DISTRESS_CRITICAL_SAR",
            "vessel_name": sub.canoe_name,
            "owner_msisdn": sub.phone_number,
            "landing_beach": sub.landing_beach,
            "mother_tongue": sub.language.upper(),
            "reason": reason,
            "declaration_timestamp": time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(now)),
            "last_known_fix": {
                "lat": last_lat,
                "lon": last_lon,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(sub.last_seen_at or now)),
            },
            "adrift_kinematics": {
                "hours_adrift": datum["hours_adrift"],
                "drift_bearing": f"{datum['drift_bearing_deg']} deg (Eastward Guinea Current)",
                "drift_speed_knots": datum["drift_speed_knots"],
                "drift_distance_nm": datum["drift_distance_nm"],
            },
            "projected_datum": {
                "datum_lat": datum["datum_lat"],
                "datum_lon": datum["datum_lon"],
                "search_radius_nm": datum["search_radius_nm"],
                "recommended_search_pattern": "EXPANDING_SQUARE_OR_CREEPING_LINE",
            },
            "border_warning": (
                "CRITICAL: Vessel projected inside Togo Territorial Waters. GMA must notify Lomé MRCC."
                if datum["togo_border_incursion"]
                else "Within Ghana EEZ."
            ),
            "dispatched_authorities": [
                "Ghana Maritime Authority (GMA) Search and Rescue Center - Accra",
                "Ghana Navy Eastern Naval Command - Tema Naval Base",
                "Volta Regional Fisheries Directorate - Keta",
            ],
        }

        record = SARIncidentRecord(
            incident_id=incident_id,
            phone_number=sub.phone_number,
            canoe_name=sub.canoe_name,
            landing_beach=sub.landing_beach,
            language=sub.language,
            declared_at=now,
            last_known_lat=last_lat,
            last_known_lon=last_lon,
            last_seen_at=sub.last_seen_at or now,
            datum_lat=datum["datum_lat"],
            datum_lon=datum["datum_lon"],
            drift_distance_nm=datum["drift_distance_nm"],
            search_radius_nm=datum["search_radius_nm"],
            status="ACTIVE",
            gma_docket=json.dumps(docket, indent=2),
        )

        self.store.create_sar_incident(record)
        return record

    def scan_for_overdue_voyages(self) -> List[SARIncidentRecord]:
        """Scan active voyages and declare SAR for those past expected return window."""
        active_voyages = self.store.get_active_voyages()
        now = time.time()
        declared: List[SARIncidentRecord] = []

        for v in active_voyages:
            if v.status == "ACTIVE" and now > v.expected_return_at:
                # Mark voyage OVERDUE
                inc = self.declare_incident(
                    phone_number=v.phone_number,
                    reason="AUTOMATED_WATCHDOG_OVERDUE_VOYAGE",
                    custom_hours_adrift=(now - v.departed_at) / 3600.0,
                )
                declared.append(inc)

        return declared
