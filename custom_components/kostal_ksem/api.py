"""
Kostal KSEM protocol implementation.

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
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

log = logging.getLogger(__name__)

CLIENT_ID = "emos"
CLIENT_SECRET = "56951025"


# ── OBIS Code Utilities ───────────────────────────────────────────────────────

def encode_obis(media: int, channel: int, indicator: int, mode: int,
                quantities: int, storage: int) -> int:
    parts = [0, 0, media, channel, indicator, mode, quantities, storage]
    result = 0
    for b in parts:
        result = result * 256 + b
    return result


def decode_obis(code: int) -> Tuple[int, int, int, int, int, int]:
    b = []
    for _ in range(8):
        b.append(code & 0xFF)
        code >>= 8
    storage, quantities, mode, indicator, channel, media = b[0], b[1], b[2], b[3], b[4], b[5]
    return media, channel, indicator, mode, quantities, storage


# ── OBIS Code Catalog ─────────────────────────────────────────────────────────

@dataclass
class OBISMeta:
    name: str
    description: str
    unit: str
    scale: float
    device_class: str = ""


OBIS_CATALOG: Dict[int, OBISMeta] = {}


def _add(media, channel, indicator, mode, quantities, storage,
         name, description, unit, scale, device_class=""):
    code = encode_obis(media, channel, indicator, mode, quantities, storage)
    OBIS_CATALOG[code] = OBISMeta(name, description, unit, scale, device_class)


# Active Power (mW raw)
_add(1,0,1,4,0,255,  "active_power_positive",      "Total active power drawn (+, import)", "W",  0.001, "power")
_add(1,0,2,4,0,255,  "active_power_negative",      "Total active power fed (-, export)",   "W",  0.001, "power")
_add(1,0,21,4,0,255, "active_power_l1_positive",   "L1 active power positive (import)",    "W",  0.001, "power")
_add(1,0,22,4,0,255, "active_power_l1_negative",   "L1 active power negative (export)",    "W",  0.001, "power")
_add(1,0,41,4,0,255, "active_power_l2_positive",   "L2 active power positive (import)",    "W",  0.001, "power")
_add(1,0,42,4,0,255, "active_power_l2_negative",   "L2 active power negative (export)",    "W",  0.001, "power")
_add(1,0,61,4,0,255, "active_power_l3_positive",   "L3 active power positive (import)",    "W",  0.001, "power")
_add(1,0,62,4,0,255, "active_power_l3_negative",   "L3 active power negative (export)",    "W",  0.001, "power")

# Reactive Power
_add(1,0,3,4,0,255,  "reactive_power_positive",    "Total reactive power positive (ind)",  "var", 0.001, "reactive_power")
_add(1,0,4,4,0,255,  "reactive_power_negative",    "Total reactive power negative (cap)",  "var", 0.001, "reactive_power")
_add(1,0,23,4,0,255, "reactive_power_l1_positive", "L1 reactive power positive",           "var", 0.001, "reactive_power")
_add(1,0,24,4,0,255, "reactive_power_l1_negative", "L1 reactive power negative",           "var", 0.001, "reactive_power")
_add(1,0,43,4,0,255, "reactive_power_l2_positive", "L2 reactive power positive",           "var", 0.001, "reactive_power")
_add(1,0,44,4,0,255, "reactive_power_l2_negative", "L2 reactive power negative",           "var", 0.001, "reactive_power")
_add(1,0,63,4,0,255, "reactive_power_l3_positive", "L3 reactive power positive",           "var", 0.001, "reactive_power")
_add(1,0,64,4,0,255, "reactive_power_l3_negative", "L3 reactive power negative",           "var", 0.001, "reactive_power")

# Apparent Power (mVA raw)
_add(1,0,9,4,0,255,  "apparent_power_positive",    "Total apparent power positive",        "VA",  0.001, "apparent_power")
_add(1,0,10,4,0,255, "apparent_power_negative",    "Total apparent power negative",        "VA",  0.001, "apparent_power")
_add(1,0,29,4,0,255, "apparent_power_l1_positive", "L1 apparent power positive",           "VA",  0.001, "apparent_power")
_add(1,0,30,4,0,255, "apparent_power_l1_negative", "L1 apparent power negative",           "VA",  0.001, "apparent_power")
_add(1,0,49,4,0,255, "apparent_power_l2_positive", "L2 apparent power positive",           "VA",  0.001, "apparent_power")
_add(1,0,50,4,0,255, "apparent_power_l2_negative", "L2 apparent power negative",           "VA",  0.001, "apparent_power")
_add(1,0,69,4,0,255, "apparent_power_l3_positive", "L3 apparent power positive",           "VA",  0.001, "apparent_power")
_add(1,0,70,4,0,255, "apparent_power_l3_negative", "L3 apparent power negative",           "VA",  0.001, "apparent_power")

# Current (mA raw)
_add(1,0,31,4,0,255, "current_l1",  "L1 current",  "A", 0.001, "current")
_add(1,0,51,4,0,255, "current_l2",  "L2 current",  "A", 0.001, "current")
_add(1,0,71,4,0,255, "current_l3",  "L3 current",  "A", 0.001, "current")

# Voltage (mV raw)
_add(1,0,32,4,0,255, "voltage_l1",  "L1 voltage",  "V", 0.001, "voltage")
_add(1,0,52,4,0,255, "voltage_l2",  "L2 voltage",  "V", 0.001, "voltage")
_add(1,0,72,4,0,255, "voltage_l3",  "L3 voltage",  "V", 0.001, "voltage")

# Power Factor
_add(1,0,13,4,0,255, "power_factor_total", "Total power factor (cos φ)", "", 0.001, "power_factor")
_add(1,0,33,4,0,255, "power_factor_l1",    "L1 power factor",             "", 0.001, "power_factor")
_add(1,0,53,4,0,255, "power_factor_l2",    "L2 power factor",             "", 0.001, "power_factor")
_add(1,0,73,4,0,255, "power_factor_l3",    "L3 power factor",             "", 0.001, "power_factor")

# Frequency (mHz raw)
_add(1,0,14,4,0,255, "frequency", "Grid frequency", "Hz", 0.001, "frequency")

# Active Energy (mWh raw → kWh)
_add(1,0,1,8,0,255,  "energy_import_total",    "Total energy imported (active)",  "kWh", 1e-6, "energy")
_add(1,0,2,8,0,255,  "energy_export_total",    "Total energy exported (active)",  "kWh", 1e-6, "energy")
_add(1,0,21,8,0,255, "energy_import_l1",       "L1 energy imported",              "kWh", 1e-6, "energy")
_add(1,0,22,8,0,255, "energy_export_l1",       "L1 energy exported",              "kWh", 1e-6, "energy")
_add(1,0,41,8,0,255, "energy_import_l2",       "L2 energy imported",              "kWh", 1e-6, "energy")
_add(1,0,42,8,0,255, "energy_export_l2",       "L2 energy exported",              "kWh", 1e-6, "energy")
_add(1,0,61,8,0,255, "energy_import_l3",       "L3 energy imported",              "kWh", 1e-6, "energy")
_add(1,0,62,8,0,255, "energy_export_l3",       "L3 energy exported",              "kWh", 1e-6, "energy")

# Reactive Energy (mVArh raw → kVArh)
_add(1,0,3,8,0,255,  "reactive_energy_inductive_total",  "Total reactive energy inductive",  "kvarh", 1e-6, "")
_add(1,0,4,8,0,255,  "reactive_energy_capacitive_total", "Total reactive energy capacitive", "kvarh", 1e-6, "")
_add(1,0,23,8,0,255, "reactive_energy_inductive_l1",     "L1 reactive energy inductive",     "kvarh", 1e-6, "")
_add(1,0,24,8,0,255, "reactive_energy_capacitive_l1",    "L1 reactive energy capacitive",    "kvarh", 1e-6, "")
_add(1,0,43,8,0,255, "reactive_energy_inductive_l2",     "L2 reactive energy inductive",     "kvarh", 1e-6, "")
_add(1,0,44,8,0,255, "reactive_energy_capacitive_l2",    "L2 reactive energy capacitive",    "kvarh", 1e-6, "")
_add(1,0,63,8,0,255, "reactive_energy_inductive_l3",     "L3 reactive energy inductive",     "kvarh", 1e-6, "")
_add(1,0,64,8,0,255, "reactive_energy_capacitive_l3",    "L3 reactive energy capacitive",    "kvarh", 1e-6, "")

# Apparent Energy (mVAh raw → kVAh)
_add(1,0,9,8,0,255,  "apparent_energy_total_pos",  "Total apparent energy positive",  "kVAh", 1e-6, "")
_add(1,0,10,8,0,255, "apparent_energy_total_neg",  "Total apparent energy negative",  "kVAh", 1e-6, "")
_add(1,0,29,8,0,255, "apparent_energy_l1_pos",     "L1 apparent energy positive",     "kVAh", 1e-6, "")
_add(1,0,30,8,0,255, "apparent_energy_l1_neg",     "L1 apparent energy negative",     "kVAh", 1e-6, "")
_add(1,0,49,8,0,255, "apparent_energy_l2_pos",     "L2 apparent energy positive",     "kVAh", 1e-6, "")
_add(1,0,50,8,0,255, "apparent_energy_l2_neg",     "L2 apparent energy negative",     "kVAh", 1e-6, "")
_add(1,0,69,8,0,255, "apparent_energy_l3_pos",     "L3 apparent energy positive",     "kVAh", 1e-6, "")
_add(1,0,70,8,0,255, "apparent_energy_l3_neg",     "L3 apparent energy negative",     "kVAh", 1e-6, "")


# ── Protobuf Wire Format Decoder ──────────────────────────────────────────────

class ProtoDecodeError(Exception):
    pass


def _read_varint(data: bytes, pos: int) -> Tuple[int, int]:
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
    if v >= (1 << 63):
        v -= (1 << 64)
    return v


def _skip_field(data: bytes, pos: int, wire_type: int) -> int:
    if wire_type == 0:
        _, pos = _read_varint(data, pos)
    elif wire_type == 1:
        pos += 8
    elif wire_type == 2:
        _, pos = _read_bytes(data, pos)
    elif wire_type == 5:
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
    values: Dict[int, int] = field(default_factory=dict)
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
    gdr = GDR()
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7

        if field_num == 1 and wire_type == 2:
            gdr.id, pos = _read_string(data, pos)
        elif field_num == 2 and wire_type == 0:
            gdr.status, pos = _read_varint(data, pos)
        elif field_num == 3 and wire_type == 2:
            ts_bytes, pos = _read_bytes(data, pos)
            gdr.timestamp_seconds, gdr.timestamp_nanos = _decode_timestamp(ts_bytes)
        elif field_num == 4 and wire_type == 2:
            entry_bytes, pos = _read_bytes(data, pos)
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
        elif field_num == 5 and wire_type == 2:
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
    gdrs: Dict[str, GDR] = {}
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
            gdrs[entry_key] = _decode_gdr(entry_val_bytes)
        else:
            pos = _skip_field(data, pos, wire_type)
    return gdrs


def _decode_gcr(data: bytes) -> GCR:
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
        elif field_num == 5 and wire_type == 2:
            packed_bytes, pos = _read_bytes(data, pos)
            pp = 0
            while pp < len(packed_bytes):
                code, pp = _read_varint(packed_bytes, pp)
                gcr.codes.append(code)
        elif field_num == 5 and wire_type == 0:
            code, pos = _read_varint(data, pos)
            gcr.codes.append(code)
        elif field_num == 6 and wire_type == 0:
            gcr.device_type, pos = _read_varint(data, pos)
        elif field_num == 7 and wire_type == 2:
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
        elif field_num == 9 and wire_type == 2:
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

# Known flexValue keys: (description, scale_from_raw, unit, device_class)
FLEX_META: Dict[str, Tuple[str, float, str, str]] = {
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
    # SOC: raw intValue = whole-number percent (90 = 90%)
    "battery_soc":             ("Battery state of charge",               1.0,   "%",  "battery"),
    "state_of_charge":         ("Battery state of charge",               1.0,   "%",  "battery"),
    "systemStateOfCharge":     ("System battery state of charge",        1.0,   "%",  "battery"),
    "gridFrequency":           ("Grid frequency",                        0.001, "Hz", "frequency"),
    "gridPowerFactor":         ("Grid power factor total",               0.001, "",   "power_factor"),
    "sumInverterControlValues": ("Sum of inverter control values",        1,     "",   ""),
    "inverterCurtailment":      ("Inverter curtailment active (1=yes)",   1,     "",   ""),
}


def gdr_value_to_sensor(obis_code: int, raw: int) -> Dict[str, Any]:
    if raw >= (1 << 63):
        raw_signed = raw - (1 << 64)
    else:
        raw_signed = raw

    meta = OBIS_CATALOG.get(obis_code)
    if meta:
        return {
            "name": meta.name,
            "description": meta.description,
            "value": round(raw_signed * meta.scale, 4),
            "unit": meta.unit,
            "device_class": meta.device_class,
        }
    else:
        m, ch, ind, mode, qty, stor = decode_obis(obis_code)
        return {
            "name": f"obis_{m}_{ch}_{ind}_{mode}_{qty}_{stor}",
            "description": f"Unknown OBIS {m}-{ch}:{ind}.{mode}.{qty}*{stor}",
            "value": raw_signed,
            "unit": "",
            "device_class": "",
        }


def flex_value_to_sensor(key: str, fv: FlexValue,
                          fd: Optional[FlexDefinition] = None) -> Dict[str, Any]:
    if key in FLEX_META:
        desc, scale, unit, dc = FLEX_META[key]
        if fv.string_value:
            val = fv.string_value
        else:
            val = round(fv.int_value * scale, 4)
        return {"name": key, "description": desc, "value": val, "unit": unit,
                "device_class": dc}
    label = fd.label if fd else key
    return {"name": key, "description": label,
            "value": fv.string_value if fv.string_value else fv.int_value,
            "unit": "", "device_class": ""}


# ── Auth & HTTP ───────────────────────────────────────────────────────────────

async def authenticate(session: aiohttp.ClientSession, host: str,
                       username: str, password: str) -> str:
    """Obtain a JWT bearer token. Raises on auth failure."""
    url = f"http://{host}/api/web-login/token"
    data = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": username,
        "password": password,
    }
    async with session.post(url, data=data) as resp:
        resp.raise_for_status()
        body = await resp.json(content_type=None)
        if "access_token" not in body:
            raise RuntimeError(f"Auth failed: {body}")
        log.info("Token obtained, expires in %ds", body.get("expires_in", 0))
        return body["access_token"]


async def fetch_gcr_config(session: aiohttp.ClientSession, host: str,
                            token: str, channel: str) -> Dict[str, GCR]:
    url = f"http://{host}/api/data-transfer/protobuf/gdr/local/config/{channel}"
    headers = {"Authorization": f"Bearer {token}"}
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.read()
            return decode_gcrs(data)
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
      - smart-meter:                   Raw OBIS meter data
      - kostal-energyflow/sumvalues:   Aggregated energy flow
      - kostal-solar-electric/inverter: PV/battery inverter data
      - kostal-solar-electric/battery:  Battery unit data
      - kostal/evse:                   Wallbox / EV charger data
    """

    CHANNELS = [
        "smart-meter",
        "kostal-energyflow/sumvalues",
        "kostal-solar-electric/inverter",
        "kostal-solar-electric/battery",
        "kostal/evse",
    ]

    def __init__(self, host: str, username: str = "user", password: str = ""):
        self.host = host
        self.username = username
        self.password = password
        self.token: str = ""
        self._data: Dict[str, ChannelData] = {
            ch: ChannelData(channel=ch) for ch in self.CHANNELS
        }
        self._tasks: List[asyncio.Task] = []
        self._callbacks: List[Callable] = []

    def on_update(self, callback: Callable):
        """Register a callback(channel, device_id, gdr) called on each GDR update."""
        self._callbacks.append(callback)

    def get_channel_devices(self) -> Dict[str, Dict[str, GCR]]:
        """Return {channel: {device_id: GCR}} for channels that have devices."""
        return {ch: cd.gcr for ch, cd in self._data.items() if cd.gcr}

    def get_all_data(self) -> Dict[str, Any]:
        """Return a structured dict of all currently known data."""
        result = {}
        for channel, cd in self._data.items():
            channel_key = channel.replace("/", "_").replace("-", "_")
            result[channel_key] = {}
            for device_id, gdr in cd.gdr.items():
                gcr = cd.gcr.get(device_id)
                dev_label = gcr.label if gcr else device_id
                sensors = {}

                for obis_code, raw in gdr.values.items():
                    entry = gdr_value_to_sensor(obis_code, raw)
                    sensors[entry["name"]] = entry

                for fv_key, fv in gdr.flex_values.items():
                    fd = gcr.flex_definitions.get(fv_key) if gcr else None
                    entry = flex_value_to_sensor(fv_key, fv, fd)
                    sensors[entry["name"]] = entry

                result[channel_key][device_id] = {
                    "label": dev_label,
                    "status": gdr.status,
                    "timestamp": gdr.timestamp_seconds,
                    "sensors": sensors,
                }
        return result

    async def start(self, session: aiohttp.ClientSession):
        """Authenticate, fetch GCR configs, and start WebSocket subscriptions."""
        self.token = await authenticate(session, self.host, self.username, self.password)

        for channel in self.CHANNELS:
            gcrs = await fetch_gcr_config(session, self.host, self.token, channel)
            if gcrs:
                self._data[channel].gcr = gcrs
                log.info("GCR for %s: %d device(s) — %s", channel, len(gcrs),
                         ", ".join(f"{v.label}" for v in gcrs.values()))

        self._tasks = [
            asyncio.create_task(self._subscribe(channel))
            for channel in self.CHANNELS
        ]

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _subscribe(self, channel: str):
        ws_url = (f"ws://{self.host}/api/data-transfer/ws/protobuf"
                  f"/gdr/local/values/{channel}")
        reconnect_delay = 5
        while True:
            try:
                log.info("Connecting to %s", ws_url)
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(ws_url) as ws:
                        await ws.send_str(f"Bearer {self.token}")
                        log.info("Subscribed: %s", channel)
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
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                log.warning("WS closed: %s", channel)
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("WS error on %s: %s — retry in %ds", channel, e, reconnect_delay)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
