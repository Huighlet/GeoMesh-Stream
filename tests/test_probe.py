"""
Unit tests for the Self-Hosted Silent Radio Prober (SIP Ring-and-Drop Engine).
"""

from geomesh.probe import SilentProbeEngine, ProbeStatus, get_probe_engine


def test_silent_probe_healthy_ringing():
    engine = SilentProbeEngine()
    res = engine.probe_subscriber("0243875318")

    assert res.phone_number == "+233243875318"
    assert res.status == ProbeStatus.RINGING_ALIVE
    assert res.is_alive is True
    assert res.sip_cause_code == 180
    assert res.cost_pesewas == 0.0
    assert res.carrier_mcc_mnc == "620-01"
    assert any("SIP CANCEL" in line for line in res.signaling_trace)
    assert any("Zero-Cost" in line for line in res.signaling_trace)


def test_silent_probe_water_immersion_detection():
    engine = SilentProbeEngine()
    test_phone = "+233248884422"

    engine.simulate_water_immersion(test_phone)
    res = engine.probe_subscriber(test_phone)

    assert res.status == ProbeStatus.IMPLICIT_DETACH_ABSENT
    assert res.is_alive is False
    assert res.sip_cause_code == 480
    assert "SAR_DATUM_TRIGGER" in res.action_recommended
    assert any("Cause 20: Subscriber Absent" in line for line in res.signaling_trace)


def test_silent_probe_togo_maritime_roaming():
    engine = SilentProbeEngine()
    test_phone = "+233240003344"

    engine.simulate_togo_roaming(test_phone)
    res = engine.probe_subscriber(test_phone)

    assert res.status == ProbeStatus.ROAMING_BORDER_CROSS
    assert res.is_alive is True
    assert res.roaming_country == "TG"
    assert res.carrier_mcc_mnc == "615-01"
    assert "Togocom" in res.carrier_name
    assert "INTERNATIONAL_ROAMING_ALERT" in res.action_recommended


def test_reset_subscriber_state():
    engine = SilentProbeEngine()
    phone = "0201112233"

    engine.simulate_water_immersion(phone)
    assert engine.probe_subscriber(phone).status == ProbeStatus.IMPLICIT_DETACH_ABSENT

    engine.reset_subscriber_state(phone)
    assert engine.probe_subscriber(phone).status == ProbeStatus.RINGING_ALIVE


def test_singleton_engine_instance():
    e1 = get_probe_engine()
    e2 = get_probe_engine()
    assert e1 is e2
