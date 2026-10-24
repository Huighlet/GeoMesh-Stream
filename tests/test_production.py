"""
Tests for GeoMesh-Stream Production Extensions:
1. Durable SQLite WAL Storage Layer
2. NMEA 0183 AIS Radio Sentence Decoder
3. SAR Watchdog & Guinea Current Drift Extrapolation
4. HMAC-SHA256 Carrier Webhook Gateway
5. End-to-End REST Endpoints & Client SDK Integration
"""

import hashlib
import hmac
import json
import time
import pytest
from starlette.testclient import TestClient

from geomesh.ais import AISDecoder, AISPositionReport
from geomesh.client import GeoMeshClient
from geomesh.sar import GuineaCurrentSARModel, SARWatchdog
from geomesh.server import app, durable_store, telecom_gateway
from geomesh.storage import DurableStore, SubscriberRecord, VoyageRecord
from geomesh.telecom import Language, VesselStatus


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def sdk_client(client):
    return GeoMeshClient(base_url="http://testserver", http_client=client)


# -------------------------------------------------------------------------
# 1. Durable SQLite WAL Storage Tests
# -------------------------------------------------------------------------

def test_durable_store_sqlite_crud(tmp_path):
    db_file = str(tmp_path / "test_store.db")
    store = DurableStore(db_path=db_file)

    # Save subscriber
    sub = SubscriberRecord(
        phone_number="+233249001122",
        owner_name="Kwame Mensah",
        canoe_name="Nyamebekyere",
        landing_beach="Keta Beach",
        language="EWE",
        status="ON_SHORE",
        registered_at=time.time(),
        last_seen_at=time.time(),
    )
    store.save_subscriber(sub)

    # Retrieve subscriber
    loaded = store.get_subscriber("+233249001122")
    assert loaded is not None
    assert loaded.canoe_name == "Nyamebekyere"
    assert loaded.landing_beach == "Keta Beach"
    assert loaded.language == "EWE"

    # Start voyage
    v_id = store.start_voyage("+233249001122", expected_duration_hours=12.0)
    assert v_id.startswith("voyage-")

    active_voyages = store.get_active_voyages()
    assert any(v.voyage_id == v_id for v in active_voyages)

    # Close voyage
    closed_id = store.close_voyage("+233249001122")
    assert closed_id == v_id
    assert not any(v.voyage_id == v_id and v.status == "ACTIVE" for v in store.get_active_voyages())

    store.close()


# -------------------------------------------------------------------------
# 2. Bit-Level NMEA 0183 AIS Decoder Tests
# -------------------------------------------------------------------------

def test_ais_nmea_decoder():
    # Synthesize valid NMEA 0183 AIVDM packet for Crude Oil Tanker
    encoded = AISDecoder.encode_position_sample(
        mmsi=636019842,
        lat=5.6200,
        lon=0.0500,
        speed_knots=15.2,
        heading_deg=240.0,
    )
    assert AISDecoder.verify_checksum(encoded) is True

    # Decode
    report = AISDecoder.decode_sentence(encoded)
    assert isinstance(report, AISPositionReport)
    assert report.mmsi == 636019842
    assert abs(report.lat - 5.6200) < 0.001
    assert abs(report.lon - 0.0500) < 0.001
    assert abs(report.sog_knots - 15.2) < 0.1
    assert abs(report.heading_deg - 240.0) < 0.1

    # Map to TelemetryPing
    ping = report.to_telemetry_ping()
    assert ping.entity_id == "MMSI-636019842"
    assert abs(ping.point.lat - 5.6200) < 0.001
    assert abs(ping.point.lon - 0.0500) < 0.001


# -------------------------------------------------------------------------
# 3. SAR Guinea Current Drift Model & Watchdog Tests
# -------------------------------------------------------------------------

def test_sar_guinea_current_drift_model():
    datum = GuineaCurrentSARModel.extrapolate_datum(
        last_lat=5.8200,
        last_lon=0.8800,
        hours_adrift=6.0,
    )
    # Guinea Current flows EASTWARD towards Togo/Benin
    assert datum["datum_lon"] > 0.8800
    assert datum["drift_distance_nm"] > 8.0
    assert datum["search_radius_nm"] > GuineaCurrentSARModel.INITIAL_POSITION_ERROR_NM
    assert "togo_border_incursion" in datum


def test_sar_watchdog_incident_generation(tmp_path):
    store = DurableStore(db_path=str(tmp_path / "sar_test.db"))
    sub = SubscriberRecord(
        phone_number="+233244998877",
        owner_name="Efo Mawuli",
        canoe_name="Agbesi",
        landing_beach="Dzelukope",
        language="EWE",
        status="AT_SEA",
        registered_at=time.time() - 36000,
        last_seen_at=time.time() - 25000,
        last_lat=5.8000,
        last_lon=0.9200,
    )
    store.save_subscriber(sub)
    watchdog = SARWatchdog(store)

    incident = watchdog.declare_incident("+233244998877", reason="TEST_BREAKDOWN", custom_hours_adrift=7.0)
    assert incident.status == "ACTIVE"
    assert incident.drift_distance_nm > 9.0
    assert "DISTRESS_CRITICAL_SAR" in incident.gma_docket
    assert "Ghana Maritime Authority" in incident.gma_docket
    store.close()


# -------------------------------------------------------------------------
# 4. Carrier Webhook HMAC Verification Tests
# -------------------------------------------------------------------------

def test_carrier_webhook_hmac_processing():
    gateway = telecom_gateway
    payload = {
        "msisdn": "+233241119988",
        "tower_id": "TOWER_KETA_SEAWARD",
        "timing_advance": 12,
        "signal_dbm": -82,
    }
    raw_body = json.dumps(payload).encode("utf-8")
    secret = "ghana_telecom_shared_secret_2026"
    sig = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()

    # Valid HMAC
    res = gateway.process_carrier_webhook(
        payload=payload,
        raw_body=raw_body,
        signature_header=sig,
        secret=secret,
    )
    assert res["status"] == "AT_SEA"
    assert res["timing_advance"] == 12
    assert res["distance_offshore_km"] == 6.6

    # Invalid HMAC must raise ValueError
    with pytest.raises(ValueError, match="Invalid carrier webhook HMAC signature"):
        gateway.process_carrier_webhook(
            payload=payload,
            raw_body=raw_body,
            signature_header="bad_signature_hex",
            secret=secret,
        )


# -------------------------------------------------------------------------
# 5. REST Endpoints & Client SDK Integration Tests
# -------------------------------------------------------------------------

def test_rest_ais_nmea_ingestion(client):
    nmea = AISDecoder.encode_position_sample(
        mmsi=627009999,
        lat=5.6300,
        lon=0.0400,
        speed_knots=14.0,
        heading_deg=220.0,
    )
    resp = client.post("/v1/ais/nmea", json={"raw_nmea": nmea})
    assert resp.status_code == 200
    data = resp.json()
    assert data["decoded"] is True
    assert data["mmsi"] == 627009999
    assert data["entity_id"] == "MMSI-627009999"


def test_rest_sar_lifecycle_via_sdk(sdk_client):
    # Register fisherman
    sdk_client.register_subscriber(
        phone_number="+233240003344",
        canoe_name="Dzidula",
        landing_beach="Keta Beach",
        language="EWE",
    )

    # Declare SAR incident
    sar_resp = sdk_client.declare_sar_incident(
        phone_number="+233240003344",
        reason="ENGINE_SEIZED_AT_SEA",
        hours_adrift=6.5,
    )
    assert sar_resp["status"] == "ACTIVE"
    inc_id = sar_resp["incident_id"]

    # List incidents
    incidents = sdk_client.list_sar_incidents(active_only=True)
    assert incidents["total_incidents"] >= 1
    assert any(i["incident_id"] == inc_id for i in incidents["incidents"])

    # Resolve incident
    res = sdk_client.resolve_sar_incident(inc_id)
    assert res["resolved"] is True

    # Clear fleet
    clr = sdk_client.clear_fleet()
    assert clr["cleared"] is True


def test_weather_broadcast_endpoint(client):
    resp = client.post("/v1/weather/broadcast", json={
        "landing_beach": "Keta Beach",
        "hazard": "SWELL_WARNING",
        "wave_height_m": 2.8,
        "wind_speed_kt": 25.0,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["hazard"] == "SWELL_WARNING"
    assert "total_canoes_notified" in data
