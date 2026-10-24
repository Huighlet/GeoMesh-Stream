"""geomesh.storage: Production durable SQLite persistence layer with Write-Ahead Logging (WAL).

Provides zero-loss disk durability for registered artisanal canoes, cell handover events,
historical voyage tracking, voice call delivery logs, and Search & Rescue (SAR) incidents.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional


@dataclass
class SubscriberRecord:
    phone_number: str
    owner_name: str
    canoe_name: str
    landing_beach: str
    language: str
    status: str = "ON_SHORE"
    registered_at: float = 0.0
    last_seen_at: float = 0.0
    last_tower_id: str = ""
    last_timing_advance: int = 0
    last_lat: float = 0.0
    last_lon: float = 0.0
    distance_offshore_km: float = 0.0


@dataclass
class VoyageRecord:
    voyage_id: str
    phone_number: str
    departed_at: float
    returned_at: Optional[float] = None
    status: str = "ACTIVE"  # ACTIVE, COMPLETED, OVERDUE, SAR_DECLARED
    max_distance_km: float = 0.0
    expected_return_at: float = 0.0


@dataclass
class VoiceCallRecord:
    call_id: str
    phone_number: str
    provider: str
    provider_sid: str
    threat_vessel: str
    cpa_nm: float
    tcpa_min: float
    language: str
    status: str  # QUEUED, RINGING, ANSWERED, COMPLETED, FAILED
    created_at: float


@dataclass
class SARIncidentRecord:
    incident_id: str
    phone_number: str
    canoe_name: str
    landing_beach: str
    language: str
    declared_at: float
    last_known_lat: float
    last_known_lon: float
    last_seen_at: float
    datum_lat: float
    datum_lon: float
    drift_distance_nm: float
    search_radius_nm: float
    status: str  # ACTIVE, RESOLVED, CANCELLED
    gma_docket: str = ""


class DurableStore:
    """Thread-safe SQLite storage engine with WAL journaling."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.environ.get("GEOMESH_DB_PATH", "geomesh_production.db")
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0,
            )
            self._conn.row_factory = sqlite3.Row
            with self._conn:
                self._conn.execute("PRAGMA journal_mode=WAL;")
                self._conn.execute("PRAGMA synchronous=NORMAL;")
                self._conn.execute("PRAGMA foreign_keys=ON;")
        return self._conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._get_conn()
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS subscribers (
                        phone_number TEXT PRIMARY KEY,
                        owner_name TEXT NOT NULL,
                        canoe_name TEXT NOT NULL,
                        landing_beach TEXT NOT NULL,
                        language TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'ON_SHORE',
                        registered_at REAL NOT NULL,
                        last_seen_at REAL NOT NULL,
                        last_tower_id TEXT DEFAULT '',
                        last_timing_advance INTEGER DEFAULT 0,
                        last_lat REAL DEFAULT 0.0,
                        last_lon REAL DEFAULT 0.0,
                        distance_offshore_km REAL DEFAULT 0.0
                    );
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS voyages (
                        voyage_id TEXT PRIMARY KEY,
                        phone_number TEXT NOT NULL,
                        departed_at REAL NOT NULL,
                        returned_at REAL,
                        status TEXT NOT NULL DEFAULT 'ACTIVE',
                        max_distance_km REAL DEFAULT 0.0,
                        expected_return_at REAL NOT NULL,
                        FOREIGN KEY(phone_number) REFERENCES subscribers(phone_number)
                    );
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cell_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        phone_number TEXT NOT NULL,
                        tower_id TEXT NOT NULL,
                        timing_advance INTEGER NOT NULL,
                        signal_dbm INTEGER NOT NULL,
                        derived_lat REAL NOT NULL,
                        derived_lon REAL NOT NULL,
                        distance_km REAL NOT NULL,
                        event_type TEXT NOT NULL,
                        created_at REAL NOT NULL
                    );
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS voice_logs (
                        call_id TEXT PRIMARY KEY,
                        phone_number TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        provider_sid TEXT NOT NULL,
                        threat_vessel TEXT NOT NULL,
                        cpa_nm REAL NOT NULL,
                        tcpa_min REAL NOT NULL,
                        language TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at REAL NOT NULL
                    );
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sar_incidents (
                        incident_id TEXT PRIMARY KEY,
                        phone_number TEXT NOT NULL,
                        canoe_name TEXT NOT NULL,
                        landing_beach TEXT NOT NULL,
                        language TEXT NOT NULL,
                        declared_at REAL NOT NULL,
                        last_known_lat REAL NOT NULL,
                        last_known_lon REAL NOT NULL,
                        last_seen_at REAL NOT NULL,
                        datum_lat REAL NOT NULL,
                        datum_lon REAL NOT NULL,
                        drift_distance_nm REAL NOT NULL,
                        search_radius_nm REAL NOT NULL,
                        status TEXT NOT NULL DEFAULT 'ACTIVE',
                        gma_docket TEXT NOT NULL DEFAULT ''
                    );
                """)

                conn.execute("CREATE INDEX IF NOT EXISTS idx_voyages_phone ON voyages(phone_number);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_cell_events_phone ON cell_events(phone_number);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sar_status ON sar_incidents(status);")

    # -------------------------------------------------------------------------
    # Subscriber Operations
    # -------------------------------------------------------------------------

    def save_subscriber(self, sub: SubscriberRecord) -> None:
        with self._lock:
            conn = self._get_conn()
            with conn:
                conn.execute("""
                    INSERT INTO subscribers (
                        phone_number, owner_name, canoe_name, landing_beach, language,
                        status, registered_at, last_seen_at, last_tower_id,
                        last_timing_advance, last_lat, last_lon, distance_offshore_km
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(phone_number) DO UPDATE SET
                        owner_name = excluded.owner_name,
                        canoe_name = excluded.canoe_name,
                        landing_beach = excluded.landing_beach,
                        language = excluded.language,
                        status = excluded.status,
                        last_seen_at = excluded.last_seen_at,
                        last_tower_id = excluded.last_tower_id,
                        last_timing_advance = excluded.last_timing_advance,
                        last_lat = excluded.last_lat,
                        last_lon = excluded.last_lon,
                        distance_offshore_km = excluded.distance_offshore_km;
                """, (
                    sub.phone_number, sub.owner_name, sub.canoe_name, sub.landing_beach, sub.language,
                    sub.status, sub.registered_at, sub.last_seen_at, sub.last_tower_id,
                    sub.last_timing_advance, sub.last_lat, sub.last_lon, sub.distance_offshore_km
                ))

    def get_subscriber(self, phone_number: str) -> Optional[SubscriberRecord]:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM subscribers WHERE phone_number = ?;", (phone_number,))
            row = cursor.fetchone()
            if not row:
                return None
            return SubscriberRecord(**dict(row))

    def list_subscribers(self) -> List[SubscriberRecord]:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM subscribers ORDER BY registered_at DESC;")
            return [SubscriberRecord(**dict(row)) for row in cursor.fetchall()]

    def delete_subscriber(self, phone_number: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            with conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM subscribers WHERE phone_number = ?;", (phone_number,))
                return cursor.rowcount > 0

    # -------------------------------------------------------------------------
    # Voyage & Cell Event Operations
    # -------------------------------------------------------------------------

    def record_cell_event(
        self,
        phone_number: str,
        tower_id: str,
        timing_advance: int,
        signal_dbm: int,
        derived_lat: float,
        derived_lon: float,
        distance_km: float,
        event_type: str,
        created_at: Optional[float] = None,
    ) -> None:
        now = created_at or time.time()
        with self._lock:
            conn = self._get_conn()
            with conn:
                conn.execute("""
                    INSERT INTO cell_events (
                        phone_number, tower_id, timing_advance, signal_dbm,
                        derived_lat, derived_lon, distance_km, event_type, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    phone_number, tower_id, timing_advance, signal_dbm,
                    derived_lat, derived_lon, distance_km, event_type, now
                ))

    def start_voyage(
        self,
        phone_number: str,
        expected_duration_hours: float = 14.0,
        departed_at: Optional[float] = None,
    ) -> str:
        now = departed_at or time.time()
        voyage_id = f"voyage-{phone_number.replace('+', '')}-{int(now)}"
        expected_return_at = now + (expected_duration_hours * 3600.0)

        with self._lock:
            conn = self._get_conn()
            with conn:
                conn.execute("""
                    INSERT INTO voyages (
                        voyage_id, phone_number, departed_at, status, expected_return_at
                    ) VALUES (?, ?, ?, 'ACTIVE', ?);
                """, (voyage_id, phone_number, now, expected_return_at))
        return voyage_id

    def close_voyage(
        self,
        phone_number: str,
        returned_at: Optional[float] = None,
    ) -> Optional[str]:
        now = returned_at or time.time()
        with self._lock:
            conn = self._get_conn()
            with conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT voyage_id FROM voyages
                    WHERE phone_number = ? AND status = 'ACTIVE'
                    ORDER BY departed_at DESC LIMIT 1;
                """, (phone_number,))
                row = cursor.fetchone()
                if not row:
                    return None
                voyage_id = row["voyage_id"]
                conn.execute("""
                    UPDATE voyages
                    SET status = 'COMPLETED', returned_at = ?
                    WHERE voyage_id = ?;
                """, (now, voyage_id))
                return voyage_id

    def get_active_voyages(self) -> List[VoyageRecord]:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM voyages WHERE status IN ('ACTIVE', 'OVERDUE');")
            return [VoyageRecord(**dict(row)) for row in cursor.fetchall()]

    # -------------------------------------------------------------------------
    # Voice Call Operations
    # -------------------------------------------------------------------------

    def record_voice_call(self, record: VoiceCallRecord) -> None:
        with self._lock:
            conn = self._get_conn()
            with conn:
                conn.execute("""
                    INSERT INTO voice_logs (
                        call_id, phone_number, provider, provider_sid, threat_vessel,
                        cpa_nm, tcpa_min, language, status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    record.call_id, record.phone_number, record.provider, record.provider_sid,
                    record.threat_vessel, record.cpa_nm, record.tcpa_min, record.language,
                    record.status, record.created_at
                ))

    def update_voice_call_status(self, call_id: str, status: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            with conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE voice_logs SET status = ? WHERE call_id = ? OR provider_sid = ?;
                """, (status, call_id, call_id))
                return cursor.rowcount > 0

    def list_voice_calls(self, limit: int = 50) -> List[VoiceCallRecord]:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM voice_logs ORDER BY created_at DESC LIMIT ?;", (limit,))
            return [VoiceCallRecord(**dict(row)) for row in cursor.fetchall()]

    # -------------------------------------------------------------------------
    # Search and Rescue (SAR) Incident Operations
    # -------------------------------------------------------------------------

    def create_sar_incident(self, incident: SARIncidentRecord) -> None:
        with self._lock:
            conn = self._get_conn()
            with conn:
                conn.execute("""
                    INSERT INTO sar_incidents (
                        incident_id, phone_number, canoe_name, landing_beach, language,
                        declared_at, last_known_lat, last_known_lon, last_seen_at,
                        datum_lat, datum_lon, drift_distance_nm, search_radius_nm,
                        status, gma_docket
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(incident_id) DO UPDATE SET
                        datum_lat = excluded.datum_lat,
                        datum_lon = excluded.datum_lon,
                        drift_distance_nm = excluded.drift_distance_nm,
                        search_radius_nm = excluded.search_radius_nm,
                        status = excluded.status,
                        gma_docket = excluded.gma_docket;
                """, (
                    incident.incident_id, incident.phone_number, incident.canoe_name,
                    incident.landing_beach, incident.language, incident.declared_at,
                    incident.last_known_lat, incident.last_known_lon, incident.last_seen_at,
                    incident.datum_lat, incident.datum_lon, incident.drift_distance_nm,
                    incident.search_radius_nm, incident.status, incident.gma_docket
                ))

    def get_sar_incident(self, incident_id: str) -> Optional[SARIncidentRecord]:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sar_incidents WHERE incident_id = ?;", (incident_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return SARIncidentRecord(**dict(row))

    def list_sar_incidents(self, active_only: bool = True) -> List[SARIncidentRecord]:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            if active_only:
                cursor.execute("SELECT * FROM sar_incidents WHERE status = 'ACTIVE' ORDER BY declared_at DESC;")
            else:
                cursor.execute("SELECT * FROM sar_incidents ORDER BY declared_at DESC;")
            return [SARIncidentRecord(**dict(row)) for row in cursor.fetchall()]

    def resolve_sar_incident(self, incident_id: str, new_status: str = "RESOLVED") -> bool:
        with self._lock:
            conn = self._get_conn()
            with conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE sar_incidents SET status = ? WHERE incident_id = ?;
                """, (new_status, incident_id))
                return cursor.rowcount > 0

    def close(self) -> None:
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None
