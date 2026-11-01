"""
Autonomous Visual E2E Test Suite for GeoMesh-Stream.
Uses Playwright to interactively drive the GH-MARSAR Command Center,
validate DOM states, exercise radar interactions, and capture high-res screenshots
at each step of the user journey.
"""

import os
import time
from pathlib import Path
import pytest
from playwright.sync_api import sync_playwright

SCREENSHOT_DIR = Path(r"C:\Users\HUIGHLET\Desktop\GeoMesh-Stream\test_screenshots")
BASE_URL = "http://127.0.0.1:8008"


def test_autonomous_visual_e2e_suite():
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1600, "height": 900})
        page = context.new_page()

        # -------------------------------------------------------------
        # STEP 1: Command Center Launch & National Basemap Rendering
        # -------------------------------------------------------------
        page.goto(BASE_URL)
        page.wait_for_selector("#map")
        page.wait_for_selector("#topbar")
        # Give Leaflet tiles and spatial boundaries time to render cleanly
        time.sleep(2.5)

        title = page.title()
        assert "GH-MARSAR" in title
        assert page.is_visible("#statEnrolled")

        snap1 = SCREENSHOT_DIR / "01_dashboard_command_center.png"
        page.screenshot(path=str(snap1))
        assert snap1.exists()

        # -------------------------------------------------------------
        # STEP 2: One-Time Keypad Canoe Enrollment
        # -------------------------------------------------------------
        test_phone = "0245557788"
        test_canoe = "Enyonam"
        
        # Switch to Operations & Registration tab
        page.click("button:has-text('⚡ OPS')")
        time.sleep(0.5)

        page.fill("#regPhone", test_phone)
        page.fill("#regCanoe", test_canoe)
        page.select_option("#regBeach", "Keta Beach")
        page.select_option("#regLang", "EWE")

        # Click the registration button
        page.click("button:has-text('Register Canoe Once')")
        time.sleep(2.0)

        # Switch to Fleet Roster tab to verify entry
        page.click("button:has-text('🛶 FLEET')")
        time.sleep(1.0)
        roster_text = page.inner_text("#fleetTable")
        assert test_canoe in roster_text

        snap2 = SCREENSHOT_DIR / "02_canoe_registered_and_roster.png"
        page.screenshot(path=str(snap2))
        assert snap2.exists()

        # -------------------------------------------------------------
        # STEP 3: AIS Commercial Tanker Convergence & Robocall
        # -------------------------------------------------------------
        # Switch to Operations tab to inject tanker
        page.click("button:has-text('⚡ OPS')")
        time.sleep(0.5)
        page.click("button:has-text('Inject AIS Tanker (Tema VHF)')")
        time.sleep(3.0)

        # Verify collision warning or radar update in event log
        log_text = page.inner_text("#eventLog")
        assert "COLLISION" in log_text or "AIS" in log_text

        snap3 = SCREENSHOT_DIR / "03_arpa_collision_warning_and_robocall.png"
        page.screenshot(path=str(snap3))
        assert snap3.exists()

        # -------------------------------------------------------------
        # STEP 4: Real-Time Relocation Resolution (Keta -> Vodza Beach)
        # -------------------------------------------------------------
        # Switch to Locator tab
        page.click("button:has-text('🛰️ LOCATOR')")
        time.sleep(0.5)
        page.click("button:has-text('Relocate to Vodza (Sell Fish)')")
        time.sleep(3.0)

        # Check locator result box displays SAFE ON SHORE at Vodza
        locate_text = page.inner_text("#locateResultBox")
        assert "SAFE ON SHORE" in locate_text
        assert "Vodza" in locate_text
        assert "KAFUI" in locate_text or "Ewe" in locate_text or "Mother Tongue" in locate_text

        snap4 = SCREENSHOT_DIR / "04_keta_to_vodza_relocation_verified.png"
        page.screenshot(path=str(snap4))
        assert snap4.exists()

        # -------------------------------------------------------------
        # STEP 5: Emergency Keypad SOS & Guinea Current SAR Datum
        # -------------------------------------------------------------
        # Switch to Operations tab to trigger SOS
        page.click("button:has-text('⚡ OPS')")
        time.sleep(0.5)
        page.click("button:has-text('Trigger Keypad SOS')")
        time.sleep(2.0)

        # Switch to SAR tab to declare emergency search
        page.click("button:has-text('🚨 SAR HOTLINE')")
        time.sleep(0.5)
        page.fill("#sarInput", test_canoe)
        page.click("button:has-text('DECLARE SAR')")
        time.sleep(3.0)

        sar_box = page.inner_text("#activeSARBox")
        assert test_canoe in sar_box or "Datum" in sar_box or "adโป" in sar_box or "NM" in sar_box

        snap5 = SCREENSHOT_DIR / "05_sar_guinea_current_drift_datum.png"
        page.screenshot(path=str(snap5))
        assert snap5.exists()

        # Cleanup: unenroll test canoe
        try:
            page.evaluate(f"unenrollCanoe('+233245557788')")
        except Exception:
            pass

        browser.close()


if __name__ == "__main__":
    test_autonomous_visual_e2e_suite()
    print("All visual screenshots successfully captured!")
