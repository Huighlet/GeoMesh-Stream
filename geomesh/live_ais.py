"""
Live AISStream.io WebSocket connector for Real-Time Commercial Vessel Telemetry.
Connects to wss://stream.aisstream.io/v0/stream, subscribes to the Gulf of Guinea /
Ghanaian territorial waters, and continuously streams live ships into GeoMeshEngine.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional
import websockets

from geomesh.engine import GeoMeshEngine
from geomesh.types import TelemetryPing

logger = logging.getLogger("geomesh.live_ais")

DEFAULT_API_KEY = "1639ae6602e8a6206c913eb901f87c2ea2b7c945"

# Bounding box for Gulf of Guinea covering Ghana EEZ, anchorages, and shipping lanes:
# Top-Left: [7.0, -4.5] (Western border / Ivory Coast to inland)
# Bottom-Right: [2.5, 3.0] (Deep Gulf of Guinea south of Togo/Benin)
DEFAULT_BOUNDING_BOXES = [
    [[7.0, -4.5], [2.5, 3.0]],          # Primary Ghana & Gulf of Guinea coastal waters
    [[10.0, -15.0], [0.0, 10.0]],        # Broader West Africa maritime corridor
]


class LiveAISStreamClient:
    """Asynchronous background worker streaming real-world AIS messages into GeoMeshEngine."""

    def __init__(
        self,
        engine: GeoMeshEngine,
        api_key: str = DEFAULT_API_KEY,
        bounding_boxes: Optional[List[List[List[float]]]] = None,
    ):
        self.engine = engine
        self.api_key = api_key
        self.bounding_boxes = bounding_boxes or DEFAULT_BOUNDING_BOXES
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._vessel_metadata: Dict[str, Dict[str, Any]] = {}
        self.total_messages_received = 0
        self.total_vessels_tracked = 0
        self.last_connected_time: Optional[float] = None
        self.last_message_time: Optional[float] = None
        self.status = "DISCONNECTED"

    def get_vessel_metadata(self, entity_id: str) -> Optional[Dict[str, Any]]:
        return self._vessel_metadata.get(entity_id)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Live AISStream client background task initiated.")

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self.status = "STOPPED"
        logger.info("Live AISStream client stopped.")

    async def _run_loop(self) -> None:
        uri = "wss://stream.aisstream.io/v0/stream"
        backoff_sec = 2.0

        while self._running:
            try:
                self.status = "CONNECTING"
                logger.info(f"Connecting to live AIS WebSocket: {uri}...")
                async with websockets.connect(
                    uri,
                    ping_interval=25,
                    ping_timeout=25,
                    close_timeout=10,
                ) as ws:
                    self.status = "CONNECTED"
                    self.last_connected_time = time.time()
                    backoff_sec = 2.0  # Reset backoff on successful connect

                    # Send AISStream subscription packet
                    sub_payload = {
                        "APIKey": self.api_key,
                        "BoundingBoxes": self.bounding_boxes,
                        "FilterMessageTypes": [
                            "PositionReport",
                            "StandardClassBPositionReport",
                            "ExtendedClassBPositionReport",
                        ],
                    }
                    await ws.send(json.dumps(sub_payload))
                    logger.info("Subscribed to live AIS stream for Gulf of Guinea / West Africa.")

                    async for raw_msg in ws:
                        if not self._running:
                            break
                        self.last_message_time = time.time()
                        self.total_messages_received += 1
                        self._process_message(raw_msg)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.status = f"ERROR: {type(e).__name__}"
                logger.warning(f"AISStream WebSocket connection dropped: {e}. Retrying in {backoff_sec:.1f}s...")
                await asyncio.sleep(backoff_sec)
                backoff_sec = min(backoff_sec * 1.5, 30.0)

        self.status = "DISCONNECTED"

    def _process_message(self, raw_msg: str) -> None:
        try:
            payload = json.loads(raw_msg)
            msg_type = payload.get("MessageType")
            meta = payload.get("MetaData", {})
            mmsi = meta.get("MMSI")
            ship_name = (meta.get("ShipName") or "").strip()

            pos_msg = None
            if msg_type == "PositionReport":
                pos_msg = payload.get("Message", {}).get("PositionReport", {})
            elif msg_type == "StandardClassBPositionReport":
                pos_msg = payload.get("Message", {}).get("StandardClassBPositionReport", {})
            elif msg_type == "ExtendedClassBPositionReport":
                pos_msg = payload.get("Message", {}).get("ExtendedClassBPositionReport", {})

            if not pos_msg:
                return

            lat = pos_msg.get("Latitude")
            lon = pos_msg.get("Longitude")
            sog_knots = float(pos_msg.get("Sog") or pos_msg.get("SpeedOverGround") or 0.0)
            cog_deg = float(pos_msg.get("Cog") or pos_msg.get("CourseOverGround") or 0.0)

            if lat is None or lon is None or not mmsi or (abs(lat) < 0.001 and abs(lon) < 0.001):
                return

            entity_id = f"MMSI-{mmsi}"
            speed_mps = sog_knots * 0.514444

            # Store ship metadata
            self._vessel_metadata[entity_id] = {
                "mmsi": mmsi,
                "ship_name": ship_name if ship_name else f"VESSEL {mmsi}",
                "speed_knots": round(sog_knots, 1),
                "heading_deg": round(cog_deg, 0),
                "last_seen_utc": meta.get("time_utc") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "source": "LIVE_AISSTREAM",
            }
            self.total_vessels_tracked = len(self._vessel_metadata)

            # Ingest live ping into spatial engine
            ping = TelemetryPing.create(
                entity_id=entity_id,
                lat=lat,
                lon=lon,
                speed_mps=speed_mps,
                heading_deg=cog_deg,
                timestamp_ns=time.time_ns(),
            )
            self.engine.ingest_telemetry(ping)

        except Exception as ex:
            logger.debug(f"Error parsing live AIS message: {ex}")
