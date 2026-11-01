"""
End-to-end visual and functional test suite for GH-MARSAR VTS Command Radar & Mobile Departure Portal.
"""

import os
import pathlib
import time
import pytest
from playwright.sync_api import sync_playwright

BASE_URL = "http://localhost:8008"
SCREENSHOT_DIR = pathlib.Path(r"C:\Users\HUIGHLET\.gemini\antigravity\brain\c7384b3e-bd4f-4ecb-aea0-87c0e2274a50")


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
        time.sleep(2.5)

        title = page.title()
        assert "GH-MARSAR" in title
        assert page.is_visible("#statEnrolled")

        snap1 = SCREENSHOT_DIR / "01_dashboard_command_center.png"
        page.screenshot(path=str(snap1))
        assert snap1.exists()

        # -------------------------------------------------------------
        # STEP 2: Fleet Roster & Filters
        # -------------------------------------------------------------
        page.click("button:has-text('FLEET')")
        time.sleep(1.0)
        assert page.is_visible("#fleetTable")
        roster_text = page.inner_text("#fleetTable")
        assert "Agbe Yeye" in roster_text or "Volta" in roster_text

        # -------------------------------------------------------------
        # STEP 3: SAR Watchdog & Incident Verification
        # -------------------------------------------------------------
        page.click("button:has-text('SAR')")
        time.sleep(1.0)
        sar_box = page.inner_text("#activeSARBox")
        assert "Agbe Yeye" in sar_box or "COASTLINE SECTOR NORMAL" in sar_box or "Datum" in sar_box

        # -------------------------------------------------------------
        # STEP 4: Real-time GSM Locator
        # -------------------------------------------------------------
        page.click(".deck-tabs button:has-text('LOCATE')")
        time.sleep(0.5)
        page.fill("#locateInput", "0243875318")
        page.click("#tabContentLocate button:has-text('LOCATE')")
        time.sleep(2.0)
        locate_text = page.inner_text("#locateResultBox")
        assert "Agbe Yeye" in locate_text or "SAFE" in locate_text or "SEA" in locate_text

        # -------------------------------------------------------------
        # STEP 5: Mobile Beach Departure Portal (/launch)
        # -------------------------------------------------------------
        mob_context = browser.new_context(viewport={"width": 390, "height": 844}, is_mobile=True)
        mob_page = mob_context.new_page()
        mob_page.goto(f"{BASE_URL}/launch")
        mob_page.wait_for_selector("#preDepartureCard")

        # Test Multilingual Translation to Ewe
        mob_page.click("button:has-text('Eʋegbe')")
        time.sleep(0.5)
        assert "ƑU DZI YIYI" in mob_page.inner_text("#txtTitle")

        # Switch back to English and 1-tap depart
        mob_page.click("button:has-text('English')")
        time.sleep(0.3)
        mob_page.click("#departBtn")
        time.sleep(1.5)

        # Check Active Voyage HUD is visible
        assert mob_page.is_visible("#voyageHUD")
        assert "AT SEA" in mob_page.inner_text("#hudStatusPill")

        browser.close()


if __name__ == "__main__":
    test_autonomous_visual_e2e_suite()
