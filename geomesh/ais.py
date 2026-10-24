"""geomesh.ais: High-performance, zero-dependency bit-level NMEA 0183 AIVDM parser.

Decodes commercial ship Class-A / Class-B VHF AIS radio sentences (!AIVDM / !AIVDO)
directly into vessel kinematic state vectors and TelemetryPing records.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from geomesh.types import TelemetryPing


@dataclass
class AISPositionReport:
    """Decoded ITU-R M.1371 Class-A / Class-B Position Report (Types 1, 2, 3, 18)."""
    mmsi: int
    msg_type: int
    lat: float
    lon: float
    speed_mps: float
    heading_deg: float
    sog_knots: float
    cog_deg: float
    nav_status: int
    timestamp_sec: int
    raw_nmea: str

    def to_telemetry_ping(self) -> TelemetryPing:
        """Convert AIS position into engine TelemetryPing."""
        return TelemetryPing.create(
            entity_id=f"MMSI-{self.mmsi}",
            lat=self.lat,
            lon=self.lon,
            speed_mps=self.speed_mps,
            heading_deg=self.heading_deg,
            timestamp_ns=time.time_ns(),
        )


@dataclass
class AISStaticReport:
    """Decoded ITU-R M.1371 Static & Voyage Data Report (Type 5)."""
    mmsi: int
    imo: int
    call_sign: str
    vessel_name: str
    ship_type: int
    destination: str
    raw_nmea: str


class AISDecoder:
    """Decodes 6-bit armored ASCII NMEA 0183 AIVDM/AIVDO packets."""

    # 6-bit ASCII text table for AIS Type 5 strings
    _AIS_CHAR_TABLE = "@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_ !\"#$%&'()*+,-./0123456789:;<=>?"

    @staticmethod
    def verify_checksum(nmea_sentence: str) -> bool:
        """Verify NMEA 0183 XOR parity checksum."""
        s = nmea_sentence.strip()
        if not (s.startswith("!") or s.startswith("$")) or "*" not in s:
            return False
        content, cksum_hex = s[1:].split("*", 1)
        calc = 0
        for ch in content:
            calc ^= ord(ch)
        try:
            expected = int(cksum_hex[:2], 16)
            return calc == expected
        except ValueError:
            return False

    @staticmethod
    def _ascii_to_bits(armored_payload: str) -> List[int]:
        """Convert 6-bit ASCII armored string into a list of binary bits."""
        bits: List[int] = []
        for ch in armored_payload:
            val = ord(ch) - 48
            if val > 40:
                val -= 8
            val &= 0x3F  # 6 bits
            for shift in range(5, -1, -1):
                bits.append((val >> shift) & 1)
        return bits

    @staticmethod
    def _bits_to_uint(bits: List[int], start: int, length: int) -> int:
        """Extract unsigned integer from bit array."""
        val = 0
        for i in range(start, start + length):
            val = (val << 1) | bits[i]
        return val

    @staticmethod
    def _bits_to_int(bits: List[int], start: int, length: int) -> int:
        """Extract two's complement signed integer from bit array."""
        val = AISDecoder._bits_to_uint(bits, start, length)
        sign_bit = 1 << (length - 1)
        if val & sign_bit:
            val -= (1 << length)
        return val

    @staticmethod
    def _bits_to_text(bits: List[int], start: int, char_count: int) -> str:
        """Extract 6-bit ASCII text string from bit array."""
        result = []
        for i in range(char_count):
            char_bits = AISDecoder._bits_to_uint(bits, start + (i * 6), 6)
            if char_bits < len(AISDecoder._AIS_CHAR_TABLE):
                result.append(AISDecoder._AIS_CHAR_TABLE[char_bits])
            else:
                result.append("?")
        return "".join(result).strip("@ ")

    @classmethod
    def decode_sentence(cls, sentence: str) -> Optional[Union[AISPositionReport, AISStaticReport]]:
        """Decode single-sentence NMEA 0183 AIVDM/AIVDO message."""
        s = sentence.strip()
        if not s:
            return None

        # Verify Checksum
        if not cls.verify_checksum(s):
            return None

        parts = s.split("*")[0].split(",")
        if len(parts) < 6:
            return None

        armored_payload = parts[5]
        bits = cls._ascii_to_bits(armored_payload)
        if len(bits) < 38:
            return None

        msg_type = cls._bits_to_uint(bits, 0, 6)
        mmsi = cls._bits_to_uint(bits, 8, 30)

        # Message Types 1, 2, 3: Class A Position Report (168 bits)
        if msg_type in (1, 2, 3) and len(bits) >= 137:
            nav_status = cls._bits_to_uint(bits, 38, 4)
            sog_raw = cls._bits_to_uint(bits, 50, 10)
            sog_knots = sog_raw / 10.0 if sog_raw < 1023 else 0.0
            speed_mps = sog_knots * 0.514444

            lon_raw = cls._bits_to_int(bits, 61, 28)
            lat_raw = cls._bits_to_int(bits, 89, 27)
            lon = lon_raw / 600000.0
            lat = lat_raw / 600000.0

            cog_raw = cls._bits_to_uint(bits, 116, 12)
            cog_deg = cog_raw / 10.0 if cog_raw < 3600 else 0.0

            hdg_raw = cls._bits_to_uint(bits, 128, 9)
            heading_deg = float(hdg_raw) if hdg_raw < 511 else cog_deg

            ts_sec = cls._bits_to_uint(bits, 137, 6) if len(bits) >= 143 else 0

            return AISPositionReport(
                mmsi=mmsi,
                msg_type=msg_type,
                lat=lat,
                lon=lon,
                speed_mps=speed_mps,
                heading_deg=heading_deg,
                sog_knots=sog_knots,
                cog_deg=cog_deg,
                nav_status=nav_status,
                timestamp_sec=ts_sec,
                raw_nmea=s,
            )

        # Message Type 5: Class A Static and Voyage Data (424 bits)
        elif msg_type == 5 and len(bits) >= 420:
            imo = cls._bits_to_uint(bits, 40, 30)
            call_sign = cls._bits_to_text(bits, 70, 7)
            vessel_name = cls._bits_to_text(bits, 112, 20)
            ship_type = cls._bits_to_uint(bits, 232, 8)
            destination = cls._bits_to_text(bits, 302, 20)

            return AISStaticReport(
                mmsi=mmsi,
                imo=imo,
                call_sign=call_sign,
                vessel_name=vessel_name,
                ship_type=ship_type,
                destination=destination,
                raw_nmea=s,
            )

        return None

    @classmethod
    def encode_position_sample(
        cls,
        mmsi: int,
        lat: float,
        lon: float,
        speed_knots: float,
        heading_deg: float,
    ) -> str:
        """Helper for unit tests & field generators: synthesize a valid NMEA 0183 !AIVDM string."""
        # 168 bits total for Type 1
        bits = [0] * 168
        # Msg Type 1 = 000001
        for i in range(6):
            bits[i] = (1 >> (5 - i)) & 1
        # MMSI (bits 8..37)
        for i in range(30):
            bits[8 + i] = (mmsi >> (29 - i)) & 1
        # SOG (bits 50..59)
        sog_val = int(speed_knots * 10) & 0x3FF
        for i in range(10):
            bits[50 + i] = (sog_val >> (9 - i)) & 1
        # Lon (bits 61..88)
        lon_val = int(lon * 600000.0)
        if lon_val < 0:
            lon_val += (1 << 28)
        for i in range(28):
            bits[61 + i] = (lon_val >> (27 - i)) & 1
        # Lat (bits 89..115)
        lat_val = int(lat * 600000.0)
        if lat_val < 0:
            lat_val += (1 << 27)
        for i in range(27):
            bits[89 + i] = (lat_val >> (26 - i)) & 1
        # COG (bits 116..127)
        cog_val = int(heading_deg * 10) & 0xFFF
        for i in range(12):
            bits[116 + i] = (cog_val >> (11 - i)) & 1
        # Heading (bits 128..136)
        hdg_val = int(heading_deg) & 0x1FF
        for i in range(9):
            bits[128 + i] = (hdg_val >> (8 - i)) & 1

        # Pack into 6-bit chars
        armored = []
        for chunk_idx in range(0, 168, 6):
            chunk = 0
            for b in range(6):
                chunk = (chunk << 1) | bits[chunk_idx + b]
            if chunk < 40:
                ch = chr(chunk + 48)
            else:
                ch = chr(chunk + 56)
            armored.append(ch)
        payload = "".join(armored)

        content = f"AIVDM,1,1,,A,{payload},0"
        cksum = 0
        for c in content:
            cksum ^= ord(c)
        return f"!{content}*{cksum:02X}"
