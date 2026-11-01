"""tests.test_sdr_finder: Test suite for sovereign SDR cellular SAR search payload."""

import pytest
from geomesh.sdr_finder import (
    SDRBand,
    SDRCellularPayload,
    SDRTargetStatus,
    sdr_payload,
)


def test_sdr_stations_initialization():
    stations = sdr_payload.get_stations()
    assert len(stations) >= 3
    station_ids = [s["station_id"] for s in stations]
    assert "STATION_KETA_MAST" in station_ids
    assert "DRONE_KETA_SAR_1" in station_ids
    assert "NAVY_P30_PATROL" in station_ids


def test_sdr_single_station_scan():
    payload = SDRCellularPayload()
    fix = payload.execute_scan("0243875318", station_id="DRONE_KETA_SAR_1", band=SDRBand.GSM_900)

    assert fix.phone_number == "+233243875318"
    assert fix.status == SDRTargetStatus.TARGET_ACQUIRED
    assert fix.external_method == "3GPP_RACH_RF_TIME_OF_FLIGHT"
    assert fix.accuracy_m > 0
    assert fix.confidence_score >= 0.70
    assert -120.0 <= fix.rssi_dbm <= -40.0
    # Coordinates should be offshore Gulf of Guinea near Keta
    assert 5.75 <= fix.lat <= 5.95
    assert 0.90 <= fix.lon <= 1.20


def test_sdr_scan_with_known_position():
    payload = SDRCellularPayload()
    # Known offshore target at lat=5.8200, lon=1.0500
    fix = payload.execute_scan(
        "0243875318",
        station_id="STATION_KETA_MAST",
        known_approx_pos=(5.8200, 1.0500),
    )

    assert fix.phone_number == "+233243875318"
    # Should project within 50 meters of ground truth
    assert abs(fix.lat - 5.8200) < 0.005
    assert abs(fix.lon - 1.0500) < 0.005


def test_sdr_multi_station_triangulation():
    payload = SDRCellularPayload()
    phone = "0243875318"
    # Execute two measurements from different platforms
    payload.execute_scan(phone, station_id="STATION_KETA_MAST")
    payload.execute_scan(phone, station_id="DRONE_KETA_SAR_1")

    result = payload.triangulate(phone)
    assert result["triangulated"] is True
    assert result["phone_number"] == "+233243875318"
    assert "DRONE_KETA_SAR_1" in result["stations_used"]
    assert "STATION_KETA_MAST" in result["stations_used"]
    assert 5.75 <= result["lat"] <= 5.95
    assert 0.90 <= result["lon"] <= 1.20
    assert result["confidence_score"] >= 0.90
