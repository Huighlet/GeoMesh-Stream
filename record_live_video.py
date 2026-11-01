"""
Record Full Live Interactive UI Test with Video, Visual Cursor & Human Typing.
Saves the video directly to test_videos/ghana_marsar_realtime_verified.webm and the artifact directory.
Also captures chronological visual timeline frames for instant frame-by-frame visual audit.
"""

import time
import shutil
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE_URL = "http://127.0.0.1:8008"
PROJECT_DIR = Path(r"C:\Users\HUIGHLET\Desktop\GeoMesh-Stream")
VIDEO_DIR = PROJECT_DIR / "test_videos"
FRAMES_DIR = VIDEO_DIR / "timeline_frames"
ARTIFACT_DIR = Path(r"C:\Users\HUIGHLET\.gemini\antigravity\brain\c7384b3e-bd4f-4ecb-aea0-87c0e2274a50")

VIDEO_DIR.mkdir(parents=True, exist_ok=True)
FRAMES_DIR.mkdir(parents=True, exist_ok=True)


def record_live_interactive_test():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            slow_mo=350,
        )
        context = browser.new_context(
            record_video_dir=str(VIDEO_DIR),
            record_video_size={"width": 1440, "height": 900},
            viewport={"width": 1440, "height": 900},
        )
        page = context.new_page()

        print("[RECORDER] Loading C4ISR Maritime Command Bridge...", flush=True)
        page.goto(BASE_URL)
        page.wait_for_selector("#map")
        time.sleep(3.0)

        # Inject visible glowing tactical mouse cursor
        page.evaluate("""
        () => {
            const cursor = document.createElement('div');
            cursor.id = 'agent-mouse-pointer';
            cursor.style.position = 'fixed';
            cursor.style.width = '24px';
            cursor.style.height = '24px';
            cursor.style.borderRadius = '50%';
            cursor.style.backgroundColor = 'rgba(239, 68, 68, 0.9)';
            cursor.style.border = '2px solid #ffffff';
            cursor.style.boxShadow = '0 0 16px rgba(239, 68, 68, 1), inset 0 0 4px #fff';
            cursor.style.pointerEvents = 'none';
            cursor.style.zIndex = '999999';
            cursor.style.transition = 'all 0.35s cubic-bezier(0.25, 1, 0.5, 1)';
            cursor.style.transform = 'translate(-50%, -50%)';
            cursor.style.left = '50%';
            cursor.style.top = '50%';
            document.body.appendChild(cursor);

            window.moveCursorToCoords = (x, y) => {
                cursor.style.left = x + 'px';
                cursor.style.top = y + 'px';
                cursor.style.transform = 'translate(-50%, -50%) scale(1.4)';
                setTimeout(() => { cursor.style.transform = 'translate(-50%, -50%) scale(1)'; }, 220);
            };
        }
        """)

        def aim_and_click(selector):
            loc = page.locator(selector).first
            box = loc.bounding_box()
            if box:
                page.evaluate(f"window.moveCursorToCoords({box['x'] + box['width']/2}, {box['y'] + box['height']/2})")
            time.sleep(0.4)
            loc.click()

        def aim_and_type(selector, text, delay=100):
            loc = page.locator(selector).first
            box = loc.bounding_box()
            if box:
                page.evaluate(f"window.moveCursorToCoords({box['x'] + box['width']/2}, {box['y'] + box['height']/2})")
            time.sleep(0.4)
            loc.click()
            loc.fill("")
            loc.type(text, delay=delay)

        # Frame 1: Initial Command Center Overview
        page.screenshot(path=str(FRAMES_DIR / "frame01_overview.png"))

        print("1. Aiming mouse at OPS tab to open Operations & Enrollment deck...", flush=True)
        aim_and_click("button:has-text('OPS')")
        time.sleep(0.6)

        print("2. Aiming mouse at Phone input and typing phone number...", flush=True)
        aim_and_type("#regPhone", "024 888 4422", delay=110)
        time.sleep(0.4)

        print("3. Aiming mouse at Canoe Name input and typing 'Agbe Yeye'...", flush=True)
        aim_and_type("#regCanoe", "Agbe Yeye", delay=120)
        time.sleep(0.4)

        print("4. Selecting Keta Beach...", flush=True)
        aim_and_click("#regBeach")
        page.select_option("#regBeach", "Keta Beach")
        time.sleep(0.4)

        print("5. Selecting Ewe Language...", flush=True)
        aim_and_click("#regLang")
        page.select_option("#regLang", "EWE")
        time.sleep(0.4)

        # Frame 2: Form Completed with Visible Cursor
        page.screenshot(path=str(FRAMES_DIR / "frame02_form_filled.png"))

        print("6. Clicking 'Register Canoe Once' button...", flush=True)
        aim_and_click("button:has-text('Register Canoe Once')")
        time.sleep(2.5)

        print("7. Aiming mouse at FLEET tab to inspect real-time roster...", flush=True)
        aim_and_click("button:has-text('FLEET')")
        time.sleep(1.5)
        # Frame 3: Fleet Roster Updated
        page.screenshot(path=str(FRAMES_DIR / "frame03_fleet_roster.png"))

        print("8. Aiming mouse at LOCATOR tab and executing Keta-to-Vodza Relocation...", flush=True)
        aim_and_click("button:has-text('LOCATOR')")
        time.sleep(0.5)
        aim_and_click("button:has-text('Relocate to Vodza (Sell Fish)')")
        time.sleep(3.5)
        # Frame 4: Vodza Dark Glass Popup and Relocation Flight Path
        page.screenshot(path=str(FRAMES_DIR / "frame04_vodza_relocated_popup.png"))

        print("9. Switching to OPS tab and injecting converging AIS Tanker on radar...", flush=True)
        aim_and_click("button:has-text('OPS')")
        time.sleep(0.5)
        aim_and_click("button:has-text('Inject AIS Tanker (Tema VHF)')")
        time.sleep(4.0)
        # Frame 5: AIS Tanker Injected and ARPA Collision Line Tracked
        page.screenshot(path=str(FRAMES_DIR / "frame05_ais_tanker_collision.png"))

        print("10. Triggering Emergency Keypad SOS (*920*44#)...", flush=True)
        aim_and_click("button:has-text('Trigger Keypad SOS')")
        time.sleep(2.0)

        print("11. Switching to SAR HOTLINE tab to declare emergency drift datum...", flush=True)
        aim_and_click("button:has-text('SAR HOTLINE')")
        time.sleep(0.5)
        aim_and_type("#sarInput", "Agbe Yeye", delay=100)
        aim_and_click("button:has-text('DECLARE SAR')")
        time.sleep(4.5)
        # Frame 6: Emergency SAR Datum and Search Pattern
        page.screenshot(path=str(FRAMES_DIR / "frame06_sar_datum_rings.png"))

        # Cleanup
        try:
            page.evaluate("unenrollCanoe('+233248884422')")
        except Exception:
            pass

        time.sleep(1.5)

        # Get video path before closing context
        video_obj = page.video
        context.close()
        browser.close()

        # Find the newly recorded video
        recorded_path = None
        if video_obj:
            recorded_path = Path(video_obj.path())

        if not recorded_path or not recorded_path.exists():
            webms = sorted(VIDEO_DIR.glob("page@*.webm"), key=lambda f: f.stat().st_mtime, reverse=True)
            if webms:
                recorded_path = webms[0]

        target_video = VIDEO_DIR / "ghana_marsar_realtime_verified.webm"
        if recorded_path and recorded_path.exists():
            shutil.copy2(str(recorded_path), str(target_video))
            print(f"[RECORDER] Video finalized: {target_video} ({target_video.stat().st_size} bytes)", flush=True)
            if ARTIFACT_DIR.exists():
                artifact_video = ARTIFACT_DIR / "ghana_marsar_realtime_verified.webm"
                shutil.copy2(str(recorded_path), str(artifact_video))
                print(f"[RECORDER] Copied video to artifact directory: {artifact_video}", flush=True)


if __name__ == "__main__":
    record_live_interactive_test()
    print("SUCCESS: Live interactive video recorded and visual frames extracted!")
