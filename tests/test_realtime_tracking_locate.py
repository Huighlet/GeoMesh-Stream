"""
Tests for Real-Time GSM Canoe Tracking, Location-Based Services (LBS),
and Inter-Beach Relocation Resolution (e.g. Keta to Vodza).
"""

import pytest
from starlette.testclient import TestClient

from geomesh.server import app, get_engine
from geomesh.telecom import (
    TelecomPassiveGateway,
    VesselStatus,
    normalize_ghana_phone,
    find_nearest_beach,
)
from geomesh.types import Point


@pytest.fixture
def client():
    return TestClient(app)


def test_find_nearest_beach():
    # Test near Vodza
    vodza_pt = Point(5.9420, 1.0120)
    beach, dist_m = find_nearest_beach(vodza_pt)
    assert "Vodza" in beach
    assert dist_m < 500.0

    # Test near Keta
    keta_pt = Point(5.9200, 0.9900)
    beach, dist_m = find_nearest_beach(keta_pt)
    assert "Keta" in beach
    assert dist_m < 500.0

    # Test near Elmina
    elmina_pt = Point(5.0800, -1.3500)
    beach, dist_m = find_nearest_beach(elmina_pt)
    assert "Elmina" in beach
    assert dist_m < 500.0


def test_realtime_locate_home_beach():
    gateway = TelecomPassiveGateway()
    phone = "0240003344"
    sub = gateway.register_subscriber(
        phone_number=phone,
        canoe_name="Dzidula",
        landing_beach="Keta Beach",
        language="EWE",
    )
    assert sub.status == VesselStatus.ON_SHORE

    loc = gateway.locate_subscriber(phone)
    assert loc["phone_number"] == "+233240003344"
    assert loc["canoe_name"] == "Dzidula"
    assert loc["home_beach"] == "Keta Beach"
    assert loc["current_status"] == "ON_SHORE"
    assert loc["safety_assessment"] == "SAFE_ON_SHORE_HOME"
    assert "SAFE ON SHORE" in loc["display_message_en"]
    assert "Dzidula" in loc["display_message_en"]


def test_realtime_locate_relocated_keta_to_vodza():
    """
    Scenario: Canoe 'Dzidula' is registered at Keta Beach.
    Fisherman leaves Keta and lands at Vodza Beach (3.8 km NE) to sell catch.
    People in Keta panic thinking the canoe is missing at sea.
    Real-time GSM query checks current cell tower attachment and confirms
    the fisherman is safe on land at Vodza!
    """
    gateway = TelecomPassiveGateway()
    phone = "0240003344"
    gateway.register_subscriber(
        phone_number=phone,
        canoe_name="Dzidula",
        landing_beach="Keta Beach",
        language="EWE",
    )

    # Fisherman moves to Vodza Beach to sell fish -> phone connects to Vodza tower on shore (TA=0)
    ping_res = gateway.ping_radio(
        phone_number=phone,
        simulated_tower="TOWER_VODZA_COASTAL",
        timing_advance=0,
    )

    assert ping_res["current_status"] == "ON_SHORE"
    assert "Vodza" in ping_res["nearest_beach"]
    assert ping_res["home_beach"] == "Keta Beach"
    assert ping_res["distance_from_home_km"] >= 2.0  # Approx 3.8 km
    assert ping_res["safety_assessment"] == "SAFE_RELOCATED_LANDING"
    assert "NOT missing at sea" in ping_res["display_message_en"]
    assert "Vodza" in ping_res["display_message_en"]
    assert "KAFUI!" in ping_res["display_message_local"]
    assert "Vodza" in ping_res["display_message_local"]


def test_realtime_locate_at_sea():
    gateway = TelecomPassiveGateway()
    phone = "0240003344"
    gateway.register_subscriber(
        phone_number=phone,
        canoe_name="Dzidula",
        landing_beach="Keta Beach",
        language="EWE",
    )

    # Launch into ocean (TA=8 -> 4.4 km offshore)
    gateway.process_passive_cell_event(
        phone_number=phone,
        tower_id="TOWER_KETA_SEAWARD",
        timing_advance=8,
    )

    loc = gateway.locate_subscriber(phone)
    assert loc["current_status"] == "AT_SEA"
    assert loc["safety_assessment"] == "ACTIVE_AT_SEA"
    assert loc["distance_offshore_km"] == 4.4
    assert "actively fishing offshore" in loc["display_message_en"]


def test_realtime_locate_sos():
    gateway = TelecomPassiveGateway()
    phone = "0240003344"
    gateway.register_subscriber(
        phone_number=phone,
        canoe_name="Dzidula",
        landing_beach="Keta Beach",
        language="EWE",
    )
    gateway.trigger_sos(phone, message="ENGINE_FAILED")

    loc = gateway.locate_subscriber(phone)
    assert loc["current_status"] == "SOS_DISTRESS"
    assert loc["safety_assessment"] == "SOS_DISTRESS"
    assert "EMERGENCY DISTRESS" in loc["display_message_en"]


def test_rest_api_locate_and_ping(client):
    # Register test canoe
    reg = client.post("/v1/telecom/register", json={
        "phone_number": "0249998877",
        "canoe_name": "Sea Hunter",
        "landing_beach": "Keta Beach",
        "language": "EWE",
    })
    assert reg.status_code == 201

    # Locate via GET
    loc_resp = client.get("/v1/telecom/locate/0249998877")
    assert loc_resp.status_code == 200
    data = loc_resp.json()
    assert data["canoe_name"] == "Sea Hunter"
    assert data["home_beach"] == "Keta Beach"

    # Ping to Vodza via POST
    ping_resp = client.post(
        "/v1/telecom/ping/0249998877",
        params={"simulated_tower": "TOWER_VODZA_COASTAL", "timing_advance": 0},
    )
    assert ping_resp.status_code == 200
    ping_data = ping_resp.json()
    assert ping_data["safety_assessment"] == "SAFE_RELOCATED_LANDING"
    assert "Vodza" in ping_data["nearest_beach"]

    # Locate by Canoe Name fallback
    by_name_resp = client.get("/v1/telecom/locate/Sea%20Hunter")
    assert by_name_resp.status_code == 200
    assert by_name_resp.json()["phone_number"] == "+233249998877"

    # 404 on nonexistent canoe
    err_resp = client.get("/v1/telecom/locate/0200000000")
    assert err_resp.status_code == 404

    # Cleanup
    client.delete("/v1/telecom/subscribers/+233249998877")
