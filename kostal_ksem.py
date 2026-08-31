#!/usr/bin/env python3
"""
Kostal KSEM (Smart Energy Meter) full data extractor.

Reverse-engineered from the Kostal WebUI (kostal-energyflow app).
Uses the GDR (Generic Data Response) protobuf WebSocket API.

Protocol:
  - Auth: POST /api/web-login/token (OAuth2 password grant)
  - WebSocket: ws://<host>/api/data-transfer/ws/protobuf/gdr/local/values/<channel>
  - First WS message: send "Bearer <token>" as text
  - Subsequent messages: binary protobuf (GDRs message)
  - Config (REST): GET /api/data-transfer/protobuf/gdr/local/config/<channel>

Raw value units in GDR .values (OBIS-keyed):
  - Power: milliwatts (mW)   → divide by 1000 to get W
  - Energy: milliwatt-hours (mWh) → divide by 1_000_000 to get kWh
  - Current: milliamps (mA)  → divide by 1000 to get A
  - Voltage: millivolts (mV) → divide by 1000 to get V
  - Power factor: integer/1000 → e.g. 1000 = 1.000
  - Frequency: millihertz (mHz) → divide by 1000 to get Hz

FlexValue.intValue units:
  - Power-type values: milliwatts (mW)
  - Battery SOC: whole-number percent (90 = 90%)
  - Others: context-dependent
"""

import asyncio
import json
import logging
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("kostal_ksem")

# ── Configuration ─────────────────────────────────────────────────────────────
# Set via environment variables or edit directly (do not commit credentials).

import os as _os

DEVICE_HOST = _os.environ.get("KSEM_HOST", "192.168.0.107")
USERNAME = _os.environ.get("KSEM_USERNAME", "user")
PASSWORD = _os.environ.get("KSEM_PASSWORD", "")
CLIENT_ID = "emos"
CLIENT_SECRET = "56951025"

# ── OBIS Code Utilities ───────────────────────────────────────────────────────


def encode_obis(media: int, channel: int, indicator: int, mode: int,
                quantities: int, storage: int) -> int:
    """Encode an OBIS code tuple into a uint64 as the device does."""
    # Device encoding (from OBISCode.Encode() in the JS):
    # bytes = [storage, quantities, mode, indicator, channel, media, 0, 0]
    # result = reduce from MSB: result = result * 256 + byte (big-endian)
    parts = [0, 0, media, channel, indicator, mode, quantities, storage]
    result = 0
    for b in parts:
        result = result * 256 + b
    return result


def decode_obis(code: int) -> Tuple[int, int, int, int, int, int]:
    """Decode a uint64 OBIS code back to (media, channel, indicator, mode, quantities, storage)."""
    b = []
    for _ in range(8):
        b.append(code & 0xFF)
        code >>= 8
    # b = [storage, quantities, mode, indicator, channel, media, 0, 0]
    storage, quantities, mode, indicator, channel, media = b[0], b[1], b[2], b[3], b[4], b[5]
    return media, channel, indicator, mode, quantities, storage


def obis_string(media, channel, indicator, mode, quantities, storage) -> str:
    return f"{media}-{channel}:{indicator}.{mode}.{quantities}*{storage}"


# ── OBIS Code Catalog ─────────────────────────────────────────────────────────
# (name, description, unit, scale_to_ha_unit)
# scale_to_ha_unit: multiply raw value by this to get the HA unit
# HA units: W (power), kWh (energy), A (current), V (voltage), Hz (freq), dimensionless (PF, SOC)

@dataclass
class OBISMeta:
    name: str
    description: str
    unit: str
    scale: float
    device_class: str = ""  # HA device class


OBIS_CATALOG: Dict[int, OBISMeta] = {}


def _add(media, channel, indicator, mode, quantities, storage,
         name, description, unit, scale, device_class=""):
    code = encode_obis(media, channel, indicator, mode, quantities, storage)
    OBIS_CATALOG[code] = OBISMeta(name, description, unit, scale, device_class)


# ── Active Power (instantaneous, mW raw)
_add(1,0,1,4,0,255,  "active_power_positive",      "Total active power drawn (+, import)", "W",  0.001, "power")
_add(1,0,2,4,0,255,  "active_power_negative",      "Total active power fed (-, export)",   "W",  0.001, "power")
_add(1,0,21,4,0,255, "active_power_l1_positive",   "L1 active power positive (import)",    "W",  0.001, "power")
_add(1,0,22,4,0,255, "active_power_l1_negative",   "L1 active power negative (export)",    "W",  0.001, "power")
_add(1,0,41,4,0,255, "active_power_l2_positive",   "L2 active power positive (import)",    "W",  0.001, "power")
_add(1,0,42,4,0,255, "active_power_l2_negative",   "L2 active power negative (export)",    "W",  0.001, "power")
_add(1,0,61,4,0,255, "active_power_l3_positive",   "L3 active power positive (import)",    "W",  0.001, "power")
_add(1,0,62,4,0,255, "active_power_l3_negative",   "L3 active power negative (export)",    "W",  0.001, "power")

# ── Reactive Power (mW/mVAR raw)
_add(1,0,3,4,0,255,  "reactive_power_positive",    "Total reactive power positive (ind)",  "var", 0.001, "reactive_power")
_add(1,0,4,4,0,255,  "reactive_power_negative",    "Total reactive power negative (cap)",  "var", 0.001, "reactive_power")
_add(1,0,23,4,0,255, "reactive_power_l1_positive", "L1 reactive power positive",          "var", 0.001, "reactive_power")
_add(1,0,24,4,0,255, "reactive_power_l1_negative", "L1 reactive power negative",          "var", 0.001, "reactive_power")
_add(1,0,43,4,0,255, "reactive_power_l2_positive", "L2 reactive power positive",          "var", 0.001, "reactive_power")
_add(1,0,44,4,0,255, "reactive_power_l2_negative", "L2 reactive power negative",          "var", 0.001, "reactive_power")
_add(1,0,63,4,0,255, "reactive_power_l3_positive", "L3 reactive power positive",          "var", 0.001, "reactive_power")
_add(1,0,64,4,0,255, "reactive_power_l3_negative", "L3 reactive power negative",          "var", 0.001, "reactive_power")

# ── Apparent Power (mVA raw)
_add(1,0,9,4,0,255,  "apparent_power_positive",    "Total apparent power positive",        "VA",  0.001, "apparent_power")
_add(1,0,10,4,0,255, "apparent_power_negative",    "Total apparent power negative",        "VA",  0.001, "apparent_power")
_add(1,0,29,4,0,255, "apparent_power_l1_positive", "L1 apparent power positive",          "VA",  0.001, "apparent_power")
_add(1,0,30,4,0,255, "apparent_power_l1_negative", "L1 apparent power negative",          "VA",  0.001, "apparent_power")
_add(1,0,49,4,0,255, "apparent_power_l2_positive", "L2 apparent power positive",          "VA",  0.001, "apparent_power")
_add(1,0,50,4,0,255, "apparent_power_l2_negative", "L2 apparent power negative",          "VA",  0.001, "apparent_power")
_add(1,0,69,4,0,255, "apparent_power_l3_positive", "L3 apparent power positive",          "VA",  0.001, "apparent_power")
_add(1,0,70,4,0,255, "apparent_power_l3_negative", "L3 apparent power negative",          "VA",  0.001, "apparent_power")

# ── Current (mA raw)
_add(1,0,31,4,0,255, "current_l1",  "L1 current",  "A", 0.001, "current")
_add(1,0,51,4,0,255, "current_l2",  "L2 current",  "A", 0.001, "current")
_add(1,0,71,4,0,255, "current_l3",  "L3 current",  "A", 0.001, "current")

# ── Voltage (mV raw)
_add(1,0,32,4,0,255, "voltage_l1",  "L1 voltage",  "V", 0.001, "voltage")
_add(1,0,52,4,0,255, "voltage_l2",  "L2 voltage",  "V", 0.001, "voltage")
_add(1,0,72,4,0,255, "voltage_l3",  "L3 voltage",  "V", 0.001, "voltage")

# ── Power Factor (integer/1000 raw)
_add(1,0,13,4,0,255, "power_factor_total", "Total power factor (cos φ)", "",    0.001, "power_factor")
_add(1,0,33,4,0,255, "power_factor_l1",    "L1 power factor",            "",    0.001, "power_factor")
_add(1,0,53,4,0,255, "power_factor_l2",    "L2 power factor",            "",    0.001, "power_factor")
_add(1,0,73,4,0,255, "power_factor_l3",    "L3 power factor",            "",    0.001, "power_factor")

# ── Frequency (mHz raw)
_add(1,0,14,4,0,255, "frequency", "Grid frequency", "Hz", 0.001, "frequency")

# ── Active Energy (mWh raw → kWh)
_add(1,0,1,8,0,255,  "energy_import_total",    "Total energy imported (active)",  "kWh", 1e-6, "energy")
_add(1,0,2,8,0,255,  "energy_export_total",    "Total energy exported (active)",  "kWh", 1e-6, "energy")
_add(1,0,21,8,0,255, "energy_import_l1",       "L1 energy imported",              "kWh", 1e-6, "energy")
_add(1,0,22,8,0,255, "energy_export_l1",       "L1 energy exported",              "kWh", 1e-6, "energy")
_add(1,0,41,8,0,255, "energy_import_l2",       "L2 energy imported",              "kWh", 1e-6, "energy")
_add(1,0,42,8,0,255, "energy_export_l2",       "L2 energy exported",              "kWh", 1e-6, "energy")
_add(1,0,61,8,0,255, "energy_import_l3",       "L3 energy imported",              "kWh", 1e-6, "energy")
_add(1,0,62,8,0,255, "energy_export_l3",       "L3 energy exported",              "kWh", 1e-6, "energy")

# ── Reactive Energy (mVArh raw → kVArh)
_add(1,0,3,8,0,255,  "reactive_energy_inductive_total",  "Total reactive energy inductive",  "kvarh", 1e-6, "")
_add(1,0,4,8,0,255,  "reactive_energy_capacitive_total", "Total reactive energy capacitive", "kvarh", 1e-6, "")
_add(1,0,23,8,0,255, "reactive_energy_inductive_l1",     "L1 reactive energy inductive",     "kvarh", 1e-6, "")
_add(1,0,24,8,0,255, "reactive_energy_capacitive_l1",    "L1 reactive energy capacitive",    "kvarh", 1e-6, "")
_add(1,0,43,8,0,255, "reactive_energy_inductive_l2",     "L2 reactive energy inductive",     "kvarh", 1e-6, "")
_add(1,0,44,8,0,255, "reactive_energy_capacitive_l2",    "L2 reactive energy capacitive",    "kvarh", 1e-6, "")
_add(1,0,63,8,0,255, "reactive_energy_inductive_l3",     "L3 reactive energy inductive",     "kvarh", 1e-6, "")
_add(1,0,64,8,0,255, "reactive_energy_capacitive_l3",    "L3 reactive energy capacitive",    "kvarh", 1e-6, "")

# ── Apparent Energy (mVAh raw → kVAh)
_add(1,0,9,8,0,255,  "apparent_energy_total_pos",  "Total apparent energy positive",  "kVAh", 1e-6, "")
_add(1,0,10,8,0,255, "apparent_energy_total_neg",  "Total apparent energy negative",  "kVAh", 1e-6, "")
_add(1,0,29,8,0,255, "apparent_energy_l1_pos",     "L1 apparent energy positive",     "kVAh", 1e-6, "")
_add(1,0,30,8,0,255, "apparent_energy_l1_neg",     "L1 apparent energy negative",     "kVAh", 1e-6, "")
_add(1,0,49,8,0,255, "apparent_energy_l2_pos",     "L2 apparent energy positive",     "kVAh", 1e-6, "")
_add(1,0,50,8,0,255, "apparent_energy_l2_neg",     "L2 apparent energy negative",     "kVAh", 1e-6, "")
_add(1,0,69,8,0,255, "apparent_energy_l3_pos",     "L3 apparent energy positive",     "kVAh", 1e-6, "")
_add(1,0,70,8,0,255, "apparent_energy_l3_neg",     "L3 apparent energy negative",     "kVAh", 1e-6, "")


# ── Protobuf Wire Format Decoder ──────────────────────────────────────────────
# Manual decoder — no protoc needed. Implements the GDRs/GCRs/GDR/GCR schema
# exactly as reconstructed from the Kostal WebUI JavaScript source.

class ProtoDecodeError(Exception):
    pass


def _read_varint(data: bytes, pos: int) -> Tuple[int, int]:
    """Read a varint from data at pos. Returns (value, new_pos)."""
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ProtoDecodeError("Truncated varint")
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, pos


def _read_string(data: bytes, pos: int) -> Tuple[str, int]:
    length, pos = _read_varint(data, pos)
    s = data[pos:pos + length].decode("utf-8", errors="replace")
    return s, pos + length


def _read_bytes(data: bytes, pos: int) -> Tuple[bytes, int]:
    length, pos = _read_varint(data, pos)
    return data[pos:pos + length], pos + length


def _varint_to_int64(v: int) -> int:
    """Convert unsigned varint to signed int64 (zigzag not used here; just truncate)."""
    if v >= (1 << 63):
        v -= (1 << 64)
    return v


def _skip_field(data: bytes, pos: int, wire_type: int) -> int:
    if wire_type == 0:  # varint
        _, pos = _read_varint(data, pos)
    elif wire_type == 1:  # 64-bit
        pos += 8
    elif wire_type == 2:  # length-delimited
        _, pos = _read_bytes(data, pos)
    elif wire_type == 5:  # 32-bit
        pos += 4
    else:
        raise ProtoDecodeError(f"Unknown wire type {wire_type}")
    return pos


@dataclass
class FlexValue:
    int_value: int = 0
    string_value: str = ""


@dataclass
class FlexDefinition:
    label: str = ""
    type: int = 0
    unit: int = 0
    decimal_power: int = 0


@dataclass
class GDR:
    id: str = ""
    status: int = 0
    timestamp_seconds: int = 0
    timestamp_nanos: int = 0
    values: Dict[int, int] = field(default_factory=dict)    # obis_code -> raw_uint64
    flex_values: Dict[str, FlexValue] = field(default_factory=dict)


@dataclass
class GCR:
    id: str = ""
    label: str = ""
    klass: int = 0
    sources: List[str] = field(default_factory=list)
    codes: List[int] = field(default_factory=list)
    device_type: int = 0
    meta: Dict[str, str] = field(default_factory=dict)
    timestamp_seconds: int = 0
    flex_definitions: Dict[str, FlexDefinition] = field(default_factory=dict)


def _decode_timestamp(data: bytes) -> Tuple[int, int]:
    """Decode google.protobuf.Timestamp → (seconds, nanos)."""
    pos = 0
    seconds = 0
    nanos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7
        if field_num == 1 and wire_type == 0:
            v, pos = _read_varint(data, pos)
            seconds = _varint_to_int64(v)
        elif field_num == 2 and wire_type == 0:
            nanos, pos = _read_varint(data, pos)
        else:
            pos = _skip_field(data, pos, wire_type)
    return seconds, nanos


def _decode_flex_value(data: bytes) -> FlexValue:
    fv = FlexValue()
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7
        if field_num == 1 and wire_type == 0:
            v, pos = _read_varint(data, pos)
            fv.int_value = _varint_to_int64(v)
        elif field_num == 2 and wire_type == 2:
            fv.string_value, pos = _read_string(data, pos)
        else:
            pos = _skip_field(data, pos, wire_type)
    return fv


def _decode_flex_definition(data: bytes) -> FlexDefinition:
    fd = FlexDefinition()
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7
        if field_num == 1 and wire_type == 2:
            fd.label, pos = _read_string(data, pos)
        elif field_num == 2 and wire_type == 0:
            fd.type, pos = _read_varint(data, pos)
        elif field_num == 3 and wire_type == 0:
            fd.unit, pos = _read_varint(data, pos)
        elif field_num == 4 and wire_type == 0:
            fd.decimal_power, pos = _read_varint(data, pos)
        else:
            pos = _skip_field(data, pos, wire_type)
    return fd


def _decode_gdr(data: bytes) -> GDR:
    """Decode a single GDR message."""
    gdr = GDR()
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7

        if field_num == 1 and wire_type == 2:   # id = string
            gdr.id, pos = _read_string(data, pos)
        elif field_num == 2 and wire_type == 0:  # status = int32
            gdr.status, pos = _read_varint(data, pos)
        elif field_num == 3 and wire_type == 2:  # timestamp
            ts_bytes, pos = _read_bytes(data, pos)
            gdr.timestamp_seconds, gdr.timestamp_nanos = _decode_timestamp(ts_bytes)
        elif field_num == 4 and wire_type == 2:  # values map entry
            entry_bytes, pos = _read_bytes(data, pos)
            # Map entry: field 1 = key (uint64), field 2 = value (uint64)
            ep = 0
            key = 0
            val = 0
            while ep < len(entry_bytes):
                etag, ep = _read_varint(entry_bytes, ep)
                efn = etag >> 3
                ewt = etag & 0x7
                if efn == 1 and ewt == 0:
                    key, ep = _read_varint(entry_bytes, ep)
                elif efn == 2 and ewt == 0:
                    val, ep = _read_varint(entry_bytes, ep)
                else:
                    ep = _skip_field(entry_bytes, ep, ewt)
            gdr.values[key] = val
        elif field_num == 5 and wire_type == 2:  # flexValues map entry
            entry_bytes, pos = _read_bytes(data, pos)
            ep = 0
            fv_key = ""
            fv_val_bytes = b""
            while ep < len(entry_bytes):
                etag, ep = _read_varint(entry_bytes, ep)
                efn = etag >> 3
                ewt = etag & 0x7
                if efn == 1 and ewt == 2:
                    fv_key, ep = _read_string(entry_bytes, ep)
                elif efn == 2 and ewt == 2:
                    fv_val_bytes, ep = _read_bytes(entry_bytes, ep)
                else:
                    ep = _skip_field(entry_bytes, ep, ewt)
            gdr.flex_values[fv_key] = _decode_flex_value(fv_val_bytes)
        else:
            pos = _skip_field(data, pos, wire_type)
    return gdr


def decode_gdrs(data: bytes) -> Dict[str, GDR]:
    """Decode a GDRs message (map of device_id → GDR) from binary protobuf."""
    gdrs: Dict[str, GDR] = {}
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7
        if field_num == 1 and wire_type == 2:  # map entry
            entry_bytes, pos = _read_bytes(data, pos)
            ep = 0
            entry_key = ""
            entry_val_bytes = b""
            while ep < len(entry_bytes):
                etag, ep = _read_varint(entry_bytes, ep)
                efn = etag >> 3
                ewt = etag & 0x7
                if efn == 1 and ewt == 2:
                    entry_key, ep = _read_string(entry_bytes, ep)
                elif efn == 2 and ewt == 2:
                    entry_val_bytes, ep = _read_bytes(entry_bytes, ep)
                else:
                    ep = _skip_field(entry_bytes, ep, ewt)
            gdrs[entry_key] = _decode_gdr(entry_val_bytes)
        else:
            pos = _skip_field(data, pos, wire_type)
    return gdrs


def _decode_gcr(data: bytes) -> GCR:
    """Decode a single GCR message."""
    gcr = GCR()
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7

        if field_num == 1 and wire_type == 2:
            gcr.id, pos = _read_string(data, pos)
        elif field_num == 2 and wire_type == 2:
            gcr.label, pos = _read_string(data, pos)
        elif field_num == 3 and wire_type == 0:
            gcr.klass, pos = _read_varint(data, pos)
        elif field_num == 4 and wire_type == 2:
            src, pos = _read_string(data, pos)
            gcr.sources.append(src)
        elif field_num == 5 and wire_type == 2:  # packed uint64 array
            packed_bytes, pos = _read_bytes(data, pos)
            pp = 0
            while pp < len(packed_bytes):
                code, pp = _read_varint(packed_bytes, pp)
                gcr.codes.append(code)
        elif field_num == 5 and wire_type == 0:  # unpacked single
            code, pos = _read_varint(data, pos)
            gcr.codes.append(code)
        elif field_num == 6 and wire_type == 0:
            gcr.device_type, pos = _read_varint(data, pos)
        elif field_num == 7 and wire_type == 2:  # meta map entry
            entry_bytes, pos = _read_bytes(data, pos)
            ep = 0
            mk = ""
            mv = ""
            while ep < len(entry_bytes):
                etag, ep = _read_varint(entry_bytes, ep)
                efn = etag >> 3
                ewt = etag & 0x7
                if efn == 1 and ewt == 2:
                    mk, ep = _read_string(entry_bytes, ep)
                elif efn == 2 and ewt == 2:
                    mv, ep = _read_string(entry_bytes, ep)
                else:
                    ep = _skip_field(entry_bytes, ep, ewt)
            gcr.meta[mk] = mv
        elif field_num == 8 and wire_type == 2:
            ts_bytes, pos = _read_bytes(data, pos)
            gcr.timestamp_seconds, _ = _decode_timestamp(ts_bytes)
        elif field_num == 9 and wire_type == 2:  # flexDefinitions map entry
            entry_bytes, pos = _read_bytes(data, pos)
            ep = 0
            fdk = ""
            fdv_bytes = b""
            while ep < len(entry_bytes):
                etag, ep = _read_varint(entry_bytes, ep)
                efn = etag >> 3
                ewt = etag & 0x7
                if efn == 1 and ewt == 2:
                    fdk, ep = _read_string(entry_bytes, ep)
                elif efn == 2 and ewt == 2:
                    fdv_bytes, ep = _read_bytes(entry_bytes, ep)
                else:
                    ep = _skip_field(entry_bytes, ep, ewt)
            gcr.flex_definitions[fdk] = _decode_flex_definition(fdv_bytes)
        else:
            pos = _skip_field(data, pos, wire_type)
    return gcr


def decode_gcrs(data: bytes) -> Dict[str, GCR]:
    """Decode a GCRs message (map of device_id → GCR) from binary protobuf."""
    gcrs: Dict[str, GCR] = {}
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7
        if field_num == 1 and wire_type == 2:
            entry_bytes, pos = _read_bytes(data, pos)
            ep = 0
            entry_key = ""
            entry_val_bytes = b""
            while ep < len(entry_bytes):
                etag, ep = _read_varint(entry_bytes, ep)
                efn = etag >> 3
                ewt = etag & 0x7
                if efn == 1 and ewt == 2:
                    entry_key, ep = _read_string(entry_bytes, ep)
                elif efn == 2 and ewt == 2:
                    entry_val_bytes, ep = _read_bytes(entry_bytes, ep)
                else:
                    ep = _skip_field(entry_bytes, ep, ewt)
            gcrs[entry_key] = _decode_gcr(entry_val_bytes)
        else:
            pos = _skip_field(data, pos, wire_type)
    return gcrs


# ── Value Formatting ──────────────────────────────────────────────────────────


def gdr_value_to_ha(obis_code: int, raw: int) -> Dict[str, Any]:
    """Convert a raw GDR uint64 value to an HA-friendly dict."""
    # Raw values are unsigned uint64, but signed for power/energy
    # Signed conversion (most values use the full 64-bit range for negatives)
    if raw >= (1 << 63):
        raw_signed = raw - (1 << 64)
    else:
        raw_signed = raw

    meta = OBIS_CATALOG.get(obis_code)
    if meta:
        scaled = raw_signed * meta.scale
        return {
            "name": meta.name,
            "description": meta.description,
            "value": round(scaled, 4),
            "unit": meta.unit,
            "device_class": meta.device_class,
            "raw": raw_signed,
        }
    else:
        m, ch, ind, mode, qty, stor = decode_obis(obis_code)
        return {
            "name": f"obis_{m}_{ch}_{ind}_{mode}_{qty}_{stor}",
            "description": f"Unknown OBIS {m}-{ch}:{ind}.{mode}.{qty}*{stor}",
            "value": raw_signed,
            "unit": "?",
            "raw": raw_signed,
        }


def flex_value_to_ha(key: str, fv: FlexValue, fd: Optional[FlexDefinition] = None) -> Dict[str, Any]:
    """Convert a FlexValue to an HA-friendly dict with known scaling."""
    # Known flexValue keys and their semantics (from energyflow JS analysis)
    FLEX_META: Dict[str, Tuple[str, float, str, str]] = {
        # key: (description, scale_from_raw, unit, device_class)
        # Power values: raw intValue in milliwatts (mW), scale=0.001 → W
        "gridPowerTotal":          ("Grid power total (+ import, - export)", 0.001, "W",  "power"),
        "housePowerTotal":         ("House consumption total",               0.001, "W",  "power"),
        "pvPowerTotal":            ("PV power total",                        0.001, "W",  "power"),
        "batteryPowerTotal":       ("Battery power total (+ charge, - discharge)", 0.001, "W", "power"),
        "inverterPowerTotal":      ("Inverter power total",                  0.001, "W",  "power"),
        "wallboxPowerTotal":       ("Wallbox / EV charger power total",      0.001, "W",  "power"),
        "homeConsumptionGrid":     ("Home consumption from grid",            0.001, "W",  "power"),
        "homeConsumptionBattery":  ("Home consumption from battery",         0.001, "W",  "power"),
        "homeConsumptionPV":       ("Home consumption from PV",              0.001, "W",  "power"),
        "smartGridLimit":          ("Smart grid power limit",                0.001, "W",  "power"),
        "batteryChargeLimit":      ("Battery charge limit",                  0.001, "W",  "power"),
        "batteryDischargeLimit":   ("Battery discharge limit",               0.001, "W",  "power"),
        "total_dc_power":          ("Total DC power (PV input)",             0.001, "W",  "power"),
        "battery_power":           ("Battery DC power (- = discharging)",    0.001, "W",  "power"),
        "batteryPowerACSum":       ("Battery AC power sum (all inverters)",  0.001, "W",  "power"),
        "pvPowerACSum":            ("PV AC power sum (all inverters)",       0.001, "W",  "power"),
        "auxillaryPowerTotal":     ("Auxiliary power total",                  0.001, "W",  "power"),
        # Battery SOC: raw intValue = whole-number percent (90 = 90%), scale=1.0
        "battery_soc":             ("Battery state of charge",               1.0,   "%",  "battery"),
        "state_of_charge":         ("Battery state of charge",               1.0,   "%",  "battery"),
        "systemStateOfCharge":     ("System battery state of charge",        1.0,   "%",  "battery"),
        # Frequency / PF
        "gridFrequency":           ("Grid frequency",                        0.001, "Hz", "frequency"),
        "gridPowerFactor":         ("Grid power factor total",               0.001, "",   "power_factor"),
        # Control / status (dimensionless integers)
        "sumInverterControlValues": ("Sum of inverter control values",        1,     "",   ""),
        "inverterCurtailment":      ("Inverter curtailment active (1=yes)",   1,     "",   ""),
    }
    if key in FLEX_META:
        desc, scale, unit, dc = FLEX_META[key]
        val = fv.int_value
        if fv.string_value:
            val = fv.string_value
        else:
            val = round(val * scale, 4)
        return {"name": key, "description": desc, "value": val, "unit": unit,
                "device_class": dc}
    else:
        # Unknown flex key — return as-is, use FlexDefinition label if available
        label = fd.label if fd else key
        return {"name": key, "description": label,
                "value": fv.string_value if fv.string_value else fv.int_value,
                "unit": ""}


# ── Auth & HTTP ───────────────────────────────────────────────────────────────


async def get_token(session: aiohttp.ClientSession) -> str:
    url = f"http://{DEVICE_HOST}/api/web-login/token"
    data = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": USERNAME,
        "password": PASSWORD,
    }
    async with session.post(url, data=data) as resp:
        resp.raise_for_status()
        body = await resp.json(content_type=None)
        if "access_token" not in body:
            raise RuntimeError(f"Auth failed: {body}")
        log.info("Token obtained, expires in %ds", body.get("expires_in", 0))
        return body["access_token"]


async def fetch_gcr_config(session: aiohttp.ClientSession, token: str,
                           channel: str) -> Dict[str, GCR]:
    """Fetch GCR config (device metadata) for a channel via REST."""
    url = f"http://{DEVICE_HOST}/api/data-transfer/protobuf/gdr/local/config/{channel}"
    headers = {"Authorization": f"Bearer {token}"}
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.read()
            return decode_gcrs(data)
        else:
            log.warning("GCR config fetch failed for %s: HTTP %d", channel, resp.status)
            return {}


# ── WebSocket Subscriber ──────────────────────────────────────────────────────


@dataclass
class ChannelData:
    channel: str
    gcr: Dict[str, GCR] = field(default_factory=dict)
    gdr: Dict[str, GDR] = field(default_factory=dict)
    last_update: float = 0.0


class KostalKSEM:
    """
    Manages connections to all Kostal KSEM WebSocket channels.

    Channels:
      - smart-meter:                  Raw OBIS meter data (power, energy, V, A, PF, freq)
      - kostal-energyflow/sumvalues:  Aggregated energy flow (grid, PV, battery, house)
      - kostal-solar-electric/inverter: PV/battery/hybrid inverter data
      - kostal-solar-electric/battery:  Battery unit data
      - kostal/evse:                  Wallbox / EV charger data
    """

    CHANNELS = [
        "smart-meter",
        "kostal-energyflow/sumvalues",
        "kostal-solar-electric/inverter",
        "kostal-solar-electric/battery",
        "kostal/evse",
    ]

    def __init__(self, host: str = DEVICE_HOST):
        self.host = host
        self.token: str = ""
        self._data: Dict[str, ChannelData] = {
            ch: ChannelData(channel=ch) for ch in self.CHANNELS
        }
        self._tasks: List[asyncio.Task] = []
        self._callbacks: List[Callable] = []

    def on_update(self, callback: Callable):
        """Register a callback(channel, device_id, gdr) called on each GDR update."""
        self._callbacks.append(callback)

    def get_all_data(self) -> Dict[str, Any]:
        """Return a structured dict of all currently known data, HA-ready."""
        result = {}
        for channel, cd in self._data.items():
            channel_key = channel.replace("/", "_").replace("-", "_")
            result[channel_key] = {}
            for device_id, gdr in cd.gdr.items():
                gcr = cd.gcr.get(device_id)
                dev_label = gcr.label if gcr else device_id
                sensors = {}

                # OBIS-coded values
                for obis_code, raw in gdr.values.items():
                    entry = gdr_value_to_ha(obis_code, raw)
                    sensors[entry["name"]] = entry

                # FlexValues
                for fv_key, fv in gdr.flex_values.items():
                    fd = gcr.flex_definitions.get(fv_key) if gcr else None
                    entry = flex_value_to_ha(fv_key, fv, fd)
                    sensors[entry["name"]] = entry

                result[channel_key][device_id] = {
                    "label": dev_label,
                    "status": gdr.status,
                    "timestamp": gdr.timestamp_seconds,
                    "sensors": sensors,
                }
        return result

    async def start(self, session: aiohttp.ClientSession):
        """Authenticate and start all WebSocket subscriptions."""
        self.token = await get_token(session)

        # Fetch GCR config for all channels
        for channel in self.CHANNELS:
            gcrs = await fetch_gcr_config(session, self.token, channel)
            if gcrs:
                self._data[channel].gcr = gcrs
                log.info("GCR config for %s: %d device(s) — %s",
                         channel, len(gcrs),
                         ", ".join(f"{k}={v.label}" for k, v in gcrs.items()))

        # Start WebSocket subscribers
        self._tasks = [
            asyncio.create_task(self._subscribe(channel))
            for channel in self.CHANNELS
        ]

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _subscribe(self, channel: str):
        """Connect to a GDR WebSocket channel and process incoming messages."""
        import aiohttp
        ws_url = f"ws://{self.host}/api/data-transfer/ws/protobuf/gdr/local/values/{channel}"
        headers = {"Authorization": f"Bearer {self.token}"}

        reconnect_delay = 5
        while True:
            try:
                log.info("Connecting to %s", ws_url)
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(ws_url) as ws:
                        # Send auth token as first message (text frame)
                        await ws.send_str(f"Bearer {self.token}")
                        log.info("Subscribed to channel: %s", channel)
                        reconnect_delay = 5

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.BINARY:
                                try:
                                    gdrs = decode_gdrs(msg.data)
                                    self._data[channel].gdr.update(gdrs)
                                    self._data[channel].last_update = time.time()
                                    for device_id, gdr in gdrs.items():
                                        for cb in self._callbacks:
                                            cb(channel, device_id, gdr)
                                except Exception as e:
                                    log.error("Decode error on %s: %s", channel, e)
                            elif msg.type == aiohttp.WSMsgType.TEXT:
                                log.debug("Text msg on %s: %s", channel, msg.data[:200])
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                log.warning("WS closed on %s", channel)
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("WS error on %s: %s — reconnecting in %ds", channel, e, reconnect_delay)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)


# ── Derived / Computed Sensors ────────────────────────────────────────────────

def compute_derived(all_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute useful derived values from raw sensor data.
    These mirror what the Kostal WebUI displays on the energy flow page.
    """
    derived = {}

    # Smart meter net power (positive = import, negative = export)
    sm = all_data.get("smart_meter", {})
    for dev_id, dev in sm.items():
        s = dev.get("sensors", {})
        pp = s.get("active_power_positive", {}).get("value", 0) or 0
        pn = s.get("active_power_negative", {}).get("value", 0) or 0
        derived["grid_net_power_w"] = round(pp - pn, 2)
        derived["grid_import_power_w"] = round(pp, 2)
        derived["grid_export_power_w"] = round(pn, 2)

        ei = s.get("energy_import_total", {}).get("value", 0) or 0
        ee = s.get("energy_export_total", {}).get("value", 0) or 0
        derived["energy_import_total_kwh"] = round(ei, 4)
        derived["energy_export_total_kwh"] = round(ee, 4)
        break  # usually one smart meter

    # Energy flow summary
    ef = all_data.get("kostal_energyflow_sumvalues", {})
    for dev_id, dev in ef.items():
        s = dev.get("sensors", {})
        derived["pv_power_w"] = s.get("pvPowerTotal", {}).get("value")
        derived["battery_power_w"] = s.get("batteryPowerTotal", {}).get("value")
        derived["house_consumption_w"] = s.get("housePowerTotal", {}).get("value")
        derived["grid_power_w"] = s.get("gridPowerTotal", {}).get("value")
        derived["home_consumption_from_grid_w"] = s.get("homeConsumptionGrid", {}).get("value")
        derived["home_consumption_from_pv_w"] = s.get("homeConsumptionPV", {}).get("value")
        derived["home_consumption_from_battery_w"] = s.get("homeConsumptionBattery", {}).get("value")
        break

    return derived


# ── CLI / Demo ────────────────────────────────────────────────────────────────


async def _demo():
    """Run a one-shot data dump: connect, wait for first data, print, exit."""
    log.info("Connecting to Kostal KSEM at %s ...", DEVICE_HOST)
    received: Dict[str, bool] = {}
    done_event = asyncio.Event()
    channels_with_devices: set = set()

    def on_update(channel, device_id, gdr):
        received[channel] = True
        if channels_with_devices and channels_with_devices.issubset(received.keys()):
            done_event.set()

    async with aiohttp.ClientSession() as session:
        ksem = KostalKSEM(DEVICE_HOST)
        ksem.on_update(on_update)
        await ksem.start(session)

        # Only wait for channels that have GCR config (i.e., have connected devices)
        for ch, cd in ksem._data.items():
            if cd.gcr:
                channels_with_devices.add(ch)
        log.info("Channels with devices: %s", channels_with_devices)
        log.info("Waiting for data (up to 10s)...")
        try:
            await asyncio.wait_for(done_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            log.warning("Timeout — printing partial data. Got: %s", list(received.keys()))

        all_data = ksem.get_all_data()
        derived = compute_derived(all_data)

        print("\n" + "=" * 70)
        print("KOSTAL KSEM — All Data")
        print("=" * 70)
        print(json.dumps(all_data, indent=2, default=str))

        print("\n" + "=" * 70)
        print("Derived / Computed Sensors")
        print("=" * 70)
        print(json.dumps(derived, indent=2))

        await ksem.stop()


if __name__ == "__main__":
    asyncio.run(_demo())
