"""
GeoMesh-Stream: Passive Telecom Gateway & Zero-Touch Artisanal Fishermen Registry.
Enables one-time phone number subscription with automatic trip detection via
coastal cell tower handovers (Inland vs Seaward sectors) and Timing Advance (TA) distance derivation.
Includes multilingual automated voice call (IVR) alerts in Ewe, Ga, Fante, and English.
"""

from __future__ import annotations

import dataclasses
import enum
import math
import time
from typing import Any, Dict, List, Optional, Tuple

from geomesh.math import dead_reckoning_destination
from geomesh.types import Point, TelemetryPing


class Language(enum.StrEnum):
    EWE = "EWE"
    GA = "GA"
    FANTE = "FANTE"
    ENGLISH = "ENGLISH"


class VesselStatus(enum.StrEnum):
    ON_SHORE = "ON_SHORE"
    AT_SEA = "AT_SEA"
    ADRIFT_WARNING = "ADRIFT_WARNING"
    OVERDUE = "OVERDUE"


@dataclasses.dataclass(slots=True)
class TowerSector:
    """Coastal GSM/LTE Cell Tower Sector Antenna."""
    tower_id: str
    name: str
    lat: float
    lon: float
    sector_type: str  # "INLAND" or "SEAWARD"
    azimuth_deg: float  # Direction antenna is pointing
    beamwidth_deg: float = 120.0

    def calculate_position(self, timing_advance: int) -> Point:
        """
        Derive approximate geographical coordinate using GSM/LTE Timing Advance.
        Each Timing Advance (TA) step corresponds to 550 meters of radial distance.
        """
        if timing_advance <= 0 or self.sector_type == "INLAND":
            return Point(self.lat, self.lon)

        dist_m = float(timing_advance) * 550.0
        return dead_reckoning_destination(
            p=Point(self.lat, self.lon),
            distance_m=dist_m,
            bearing_deg=self.azimuth_deg,
        )


@dataclasses.dataclass(slots=True)
class CanoeSubscriber:
    """Artisanal Fisherman / Canoe Registry Record (One-Time Subscription)."""
    phone_number: str
    canoe_name: str
    landing_beach: str
    language: Language
    status: VesselStatus = VesselStatus.ON_SHORE
    registered_at_ns: int = 0
    last_seen_tower: Optional[str] = None
    last_timing_advance: int = 0
    last_point: Optional[Point] = None
    last_event_ts_ns: int = 0
    sea_departure_ts_ns: Optional[int] = None


class MaritimeVoiceAlertService:
    """Automated Emergency Voice Call (IVR) Robocall Dispatcher in Mother Tongues."""

    SCRIPTS = {
        Language.EWE: (
            "Nɔvi, meli gã aɖe le zɔzɔm va ɖo mia gbɔ tso atsiaƒua me. "
            "Miɖe abɔ le tsia me alo miatsi akaɖi enumake!"
        ),
        Language.GA: (
            "Nyɛmimɛi, mɛlɛ agbo ko miiba nyɛ gbɛjianɔ yɛ ŋshɔ lɛ mli. "
            "Nyɛgblaa nyɛyɔi loo nyɛtsoa kane kɛji amrɔ nɛ!"
        ),
        Language.FANTE: (
            "Nuakor, hyɛn kɛse bi reba hom do wɔ po no mu. "
            "Hom ntwe hom asau anaa hom nsɔ kanea ntɛm ara!"
        ),
        Language.ENGLISH: (
            "Urgent Marine Warning: A large commercial vessel is bearing down on your sector. "
            "Pull in your fishing gear and illuminate your lantern immediately."
        ),
    }

    @classmethod
    def generate_voice_payload(
        cls,
        phone_number: str,
        canoe_name: str,
        language: Language,
        cpa_nm: float,
        tcpa_min: float,
    ) -> Dict[str, Any]:
        """Generate automated robocall alert payload for telco voice gateway."""
        script = cls.SCRIPTS.get(language, cls.SCRIPTS[Language.ENGLISH])
        return {
            "dispatch_id": f"ivr_{int(time.time_ns())}",
            "recipient_phone": phone_number,
            "canoe_name": canoe_name,
            "language": str(language),
            "audio_message": script,
            "cpa_nautical_miles": cpa_nm,
            "tcpa_minutes": tcpa_min,
            "call_priority": "CRITICAL_EMERGENCY_OVERRIDE",
            "status": "QUEUED_FOR_GSM_VOICE_BROADCAST",
        }


class TelecomPassiveGateway:
    """
    Simulates telco passive radio signaling integration (MTN / Telecel Ghana).
    Detects ocean departure/return automatically without any daily dialing from fishermen.
    """

    def __init__(self) -> None:
        self._subscribers: Dict[str, CanoeSubscriber] = {}
        self._towers: Dict[str, TowerSector] = {}
        self._voice_service = MaritimeVoiceAlertService()
        self._init_default_coastal_towers()

    def _init_default_coastal_towers(self) -> None:
        """Initialize standard coastal cell towers along Keta, Ada, and Tema coastline."""
        towers = [
            TowerSector(
                tower_id="TOWER_KETA_SEAWARD",
                name="Keta Cape St. Paul Seaward Sector",
                lat=5.8000,
                lon=0.9000,
                sector_type="SEAWARD",
                azimuth_deg=165.0,
            ),
            TowerSector(
                tower_id="TOWER_KETA_INLAND",
                name="Keta Town Inland Sector",
                lat=5.9200,
                lon=0.9900,
                sector_type="INLAND",
                azimuth_deg=340.0,
            ),
            TowerSector(
                tower_id="TOWER_DZELUKOPE_SEAWARD",
                name="Dzelukope Coastal Seaward Sector",
                lat=5.8900,
                lon=0.9800,
                sector_type="SEAWARD",
                azimuth_deg=150.0,
            ),
            TowerSector(
                tower_id="TOWER_ADA_FOAH_SEAWARD",
                name="Ada Volta Estuary Seaward Sector",
                lat=5.7800,
                lon=0.6300,
                sector_type="SEAWARD",
                azimuth_deg=175.0,
            ),
            TowerSector(
                tower_id="TOWER_TEMA_SEAWARD",
                name="Tema Port Offshore Sector",
                lat=5.6200,
                lon=0.0100,
                sector_type="SEAWARD",
                azimuth_deg=180.0,
            ),
        ]
        for t in towers:
            self._towers[t.tower_id] = t

    def register_subscriber(
        self,
        phone_number: str,
        canoe_name: str,
        landing_beach: str = "Keta",
        language: Language = Language.EWE,
    ) -> CanoeSubscriber:
        """
        One-Time Lifetime Subscription.
        The fisherman never needs to register or dial again.
        """
        clean_phone = phone_number.strip().replace(" ", "")
        sub = CanoeSubscriber(
            phone_number=clean_phone,
            canoe_name=canoe_name.strip(),
            landing_beach=landing_beach.strip(),
            language=language,
            registered_at_ns=time.time_ns(),
        )
        self._subscribers[clean_phone] = sub
        return sub

    def get_subscriber(self, phone_number: str) -> Optional[CanoeSubscriber]:
        return self._subscribers.get(phone_number.strip().replace(" ", ""))

    def list_subscribers(self) -> List[CanoeSubscriber]:
        return list(self._subscribers.values())

    def get_tower(self, tower_id: str) -> Optional[TowerSector]:
        return self._towers.get(tower_id)

    def list_towers(self) -> List[TowerSector]:
        return list(self._towers.values())

    def process_passive_cell_event(
        self,
        phone_number: str,
        tower_id: str,
        timing_advance: int = 0,
        timestamp_ns: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Ingest passive radio network event (Handover, Location Update, or Silent Paging).
        Automatically transitions canoe state without any user interaction.
        """
        clean_phone = phone_number.strip().replace(" ", "")
        sub = self._subscribers.get(clean_phone)
        if not sub:
            # Auto-register unknown number with default profile
            sub = self.register_subscriber(clean_phone, f"Canoe-{clean_phone[-4:]}")

        tower = self._towers.get(tower_id)
        if not tower:
            raise ValueError(f"Unknown coastal tower: '{tower_id}'")

        now_ns = timestamp_ns or time.time_ns()
        derived_point = tower.calculate_position(timing_advance)

        prev_status = sub.status
        event_type = "POSITION_HEARTBEAT"

        # Automated Tripwire: Detect departure or landing passively
        if tower.sector_type == "SEAWARD" and timing_advance >= 1:
            if prev_status == VesselStatus.ON_SHORE:
                sub.status = VesselStatus.AT_SEA
                sub.sea_departure_ts_ns = now_ns
                event_type = "CANOE_AUTOMATIC_SEA_LAUNCH"
            else:
                sub.status = VesselStatus.AT_SEA
        elif tower.sector_type == "INLAND" or timing_advance == 0:
            if prev_status == VesselStatus.AT_SEA:
                sub.status = VesselStatus.ON_SHORE
                sub.sea_departure_ts_ns = None
                event_type = "CANOE_AUTOMATIC_SHORE_RETURN"
            else:
                sub.status = VesselStatus.ON_SHORE

        sub.last_seen_tower = tower_id
        sub.last_timing_advance = timing_advance
        sub.last_point = derived_point
        sub.last_event_ts_ns = now_ns

        distance_km = round((timing_advance * 550.0) / 1000.0, 2)

        return {
            "event_type": event_type,
            "phone_number": clean_phone,
            "canoe_name": sub.canoe_name,
            "status": str(sub.status),
            "tower_id": tower_id,
            "tower_name": tower.name,
            "timing_advance": timing_advance,
            "distance_offshore_km": distance_km,
            "derived_position": {"lat": derived_point.lat, "lon": derived_point.lon},
            "timestamp_ns": now_ns,
        }
