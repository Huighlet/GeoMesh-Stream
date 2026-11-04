"""
End-to-end visual and functional test suite for GH-MARSAR VTS Command Radar & Mobile Departure Portal.
"""

import os
import pathlib
import subprocess
import sys
import time
import urllib.request
import pytest
from playwright.sync_api import sync_playwright

BASE_URL = "http://127.0.0.1:8008"
SCREENSHOT_DIR = pathlib.Path(r"C:\Users\HUIGHLET\.gemini\antigravity\brain\c7384b3e-bd4f-4ecb-aea0-87c0e2274a50")


def _ensure_server_running():
    try:
        with urllib.request.urlopen(f"{BASE_URL}/health", timeout=1.5) as resp:
            if resp.status == 200:
                return None
    except Exception:
        pass

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "geomesh.server:app", "--host", "127.0.0.1", "--port", "8008"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(40):
        try:
            with urllib.request.urlopen(f"{BASE_URL}/health", timeout=1.0) as resp:
                if resp.status == 200:
                    return proc
        except Exception:
            time.sleep(0.25)
    return proc


def test_autonomous_visual_e2e_suite():
    proc = _ensure_server_running()
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={"width": 1600, "height": 900})
            page = context.new_page()

            # -------------------------------------------------------------
            # STEP 1: Command Center Launch & National Basemap Rendering
            # -------------------------------------------------------------
            page.goto(BASE_URL, wait_until="domcontentloaded")
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
            assert "undefined" not in locate_text
            snap_locate = SCREENSHOT_DIR / "no_emoji_07_radar_canoe_locate_popup.png"
            page.screenshot(path=str(snap_locate))

            # -------------------------------------------------------------
            # STEP 5: Mobile Beach Departure Portal (/launch)
            # -------------------------------------------------------------
            mob_context = browser.new_context(viewport={"width": 390, "height": 844}, is_mobile=True)
            mob_page = mob_context.new_page()
            mob_page.goto(f"{BASE_URL}/launch", wait_until="domcontentloaded")
            mob_page.wait_for_selector("#preDepartureCard")

            # Test Multilingual Translation to Ewe
            mob_page.click("button:has-text('EWE')")
            time.sleep(0.5)
            assert "YIYI" in mob_page.inner_text("#txtTitle")

            # Switch back to English and 1-tap depart
            mob_page.click("button:has-text('ENGLISH')")
            time.sleep(0.3)
            mob_page.click("#departBtn")
            time.sleep(1.5)

            # Check Active Voyage HUD is visible
            assert mob_page.is_visible("#voyageHUD")
            assert "AT SEA" in mob_page.inner_text("#hudStatusPill")

            browser.close()
    finally:
        if proc is not None:
            proc.terminate()


if __name__ == "__main__":
    test_autonomous_visual_e2e_suite()
