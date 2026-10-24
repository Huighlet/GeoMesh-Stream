"""
Unit and Integration Tests for GeoMesh-Stream Telecom Passive LBS Gateway.
Verifies One-Time Lifetime Registration, Timing Advance derivation, Automatic Handover detection,
and Mother-Tongue (Ewe, Ga, Fante) Voice Alert Generation.
"""

import pytest
from starlette.testclient import TestClient
from geomesh.server import app, telecom_gateway
from geomesh.telecom import Language, MaritimeVoiceAlertService, TelecomPassiveGateway, VesselStatus


@pytest.fixture
def test_client():
    return TestClient(app)


def test_one_time_subscriber_registration():
    gateway = TelecomPassiveGateway()
    sub = gateway.register_subscriber(
        phone_number="+233249998888",
        canoe_name="Agorkpo Star",
        landing_beach="Keta Beach",
        language=Language.EWE,
    )
    assert sub.phone_number == "+233249998888"
    assert sub.canoe_name == "Agorkpo Star"
    assert sub.status == VesselStatus.ON_SHORE
    assert sub.language == Language.EWE


def test_timing_advance_position_derivation():
    gateway = TelecomPassiveGateway()
    tower = gateway.get_tower("TOWER_KETA_SEAWARD")
    assert tower is not None
    assert tower.sector_type == "SEAWARD"

    # 1 TA step = 550m
    pt0 = tower.calculate_position(timing_advance=0)
    assert pt0.lat == tower.lat
    assert pt0.lon == tower.lon

    pt5 = tower.calculate_position(timing_advance=5)
    # 5 * 550 = 2,750m southward into Gulf of Guinea (lat should decrease since azimuth is 165°)
    assert pt5.lat < tower.lat
    assert pt5.lon > tower.lon


def test_passive_handover_zero_touch_lifecycle():
    gateway = TelecomPassiveGateway()
    phone = "+233245551234"
    gateway.register_subscriber(phone, "Nyimdzii", "Keta Beach", Language.EWE)

    # 1. Phone in town attached to inland sector -> ON_SHORE
    ev1 = gateway.process_passive_cell_event(phone, "TOWER_KETA_INLAND", timing_advance=0)
    assert ev1["status"] == "ON_SHORE"

    # 2. Phone enters ocean, attaches to seaward sector with TA=6 (3.3km) -> AT_SEA
    ev2 = gateway.process_passive_cell_event(phone, "TOWER_KETA_SEAWARD", timing_advance=6)
    assert ev2["status"] == "AT_SEA"
    assert ev2["event_type"] == "CANOE_AUTOMATIC_SEA_LAUNCH"
    assert ev2["distance_offshore_km"] == 3.3

    # 3. Phone returns to shore, attaches back to inland sector -> ON_SHORE
    ev3 = gateway.process_passive_cell_event(phone, "TOWER_KETA_INLAND", timing_advance=0)
    assert ev3["status"] == "ON_SHORE"
    assert ev3["event_type"] == "CANOE_AUTOMATIC_SHORE_RETURN"


def test_multilingual_voice_alert_scripts():
    # Ewe
    ewe_payload = MaritimeVoiceAlertService.generate_voice_payload(
        phone_number="+233241112222",
        canoe_name="Nyimdzii",
        language=Language.EWE,
        cpa_nm=0.8,
        tcpa_min=12.0,
    )
    assert "Nɔvi, meli gã aɖe le zɔzɔm" in ewe_payload["audio_message"]
    assert ewe_payload["language"] == "EWE"

    # Ga
    ga_payload = MaritimeVoiceAlertService.generate_voice_payload(
        phone_number="+233243334444",
        canoe_name="Oshu",
        language=Language.GA,
        cpa_nm=0.5,
        tcpa_min=7.0,
    )
    assert "Nyɛmimɛi, mɛlɛ agbo ko" in ga_payload["audio_message"]

    # Fante
    fante_payload = MaritimeVoiceAlertService.generate_voice_payload(
        phone_number="+233245556666",
        canoe_name="Bosomtwi",
        language=Language.FANTE,
        cpa_nm=1.0,
        tcpa_min=14.0,
    )
    assert "Nuakor, hyɛn kɛse bi" in fante_payload["audio_message"]


def test_telecom_rest_endpoints(test_client):
    # 1. Register Subscriber
    reg_resp = test_client.post("/v1/telecom/register", json={
        "phone_number": "0240001122",
        "canoe_name": "Volta King",
        "landing_beach": "Ada Foah",
        "language": "EWE",
    })
    assert reg_resp.status_code == 201
    assert reg_resp.json()["canoe_name"] == "Volta King"

    # 2. List Subscribers
    list_resp = test_client.get("/v1/telecom/subscribers")
    assert list_resp.status_code == 200
    assert list_resp.json()["total_subscribers"] >= 1

    # 3. Ingest Cell Event
    ev_resp = test_client.post("/v1/telecom/cell-event", json={
        "phone_number": "0240001122",
        "tower_id": "TOWER_KETA_SEAWARD",
        "timing_advance": 4,
    })
    assert ev_resp.status_code == 200
    assert ev_resp.json()["status"] == "AT_SEA"

    # 4. Dispatch Voice Alert
    voice_resp = test_client.post("/v1/telecom/alert/voice", json={
        "phone_number": "0240001122",
        "cpa_nm": 0.9,
        "tcpa_min": 10.5,
    })
    assert voice_resp.status_code == 200
    assert "audio_message" in voice_resp.json()
