@echo off
title GH-MARSAR Live Interactive Radar Test
cd /d "C:\Users\HUIGHLET\Desktop\GeoMesh-Stream"
echo Starting GH-MARSAR Live Interactive Radar Test on your screen...
".venv\Scripts\python.exe" run_live_browser_test.py
pause
