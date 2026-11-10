"""Opt-in `get_live_vehicle_status`: TeslaMate's retained MQTT state, read-only.

TeslaMate publishes every live field (locked, sentry_mode, windows_open,
doors, trunks, charging, climate, TPMS, ...) to `teslamate/cars/<id>/<field>`
as *retained* MQTT messages; the database has no columns for several of them.
This tool connects to the broker, subscribes to that one car's topics, reads
the retained values the broker replays immediately, and disconnects.

Safety properties (by construction):
- It only ever sends CONNECT, SUBSCRIBE (QoS 0), PINGREQ-free DISCONNECT; it
  never PUBLISHes, so it cannot command the car or alter TeslaMate.
- It talks to the MQTT broker only. It never contacts Tesla or TeslaMateAPI,
  so it cannot wake the car: values are TeslaMate's last-known state.
- Stdlib only (asyncio streams + a ~60-line MQTT 3.1.1 subset), no new deps.
- Registered only when MQTT_HOST is set.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import struct
from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.types import ToolAnnotations
from pydantic import Field

from ..db import fetch_all

logger = logging.getLogger(__name__)

# Fields surfaced first in the summary (the ones the database lacks or that
# users ask about most). Everything else is returned under `all_fields`.
SUMMARY_FIELDS = (
    "state",
    "since",
    "healthy",
    "locked",
    "sentry_mode",
    "windows_open",
    "driver_front_window_open",
    "driver_rear_window_open",
    "passenger_front_window_open",
    "passenger_rear_window_open",
    "doors_open",
    "driver_front_door_open",
    "driver_rear_door_open",
    "passenger_front_door_open",
    "passenger_rear_door_open",
    "trunk_open",
    "frunk_open",
    "is_user_present",
    "battery_level",
    "usable_battery_level",
    "rated_battery_range_km",
    "est_battery_range_km",
    "plugged_in",
    "charging_state",
    "charger_power",
    "charge_limit_soc",
    "time_to_full_charge",
    "charge_port_door_open",
    "is_climate_on",
    "is_preconditioning",
    "inside_temp",
    "outside_temp",
    "climate_keeper_mode",
    "tpms_pressure_fl",
    "tpms_pressure_fr",
    "tpms_pressure_rl",
    "tpms_pressure_rr",
    "tpms_soft_warning_fl",
    "tpms_soft_warning_fr",
    "tpms_soft_warning_rl",
    "tpms_soft_warning_rr",
    "geofence",
    "odometer",
    "version",
    "update_available",
    "update_version",
    "center_display_state",
)

_DB_FRESHNESS_SQL = """
SELECT
    (SELECT state::text FROM states WHERE car_id = %(car_id)s
       ORDER BY start_date DESC LIMIT 1) AS db_state,
    (SELECT start_date FROM states WHERE car_id = %(car_id)s
       ORDER BY start_date DESC LIMIT 1) AS db_state_since,
    (SELECT date FROM positions WHERE car_id = %(car_id)s
       ORDER BY date DESC LIMIT 1) AS db_last_position_at;
"""


class MqttError(RuntimeError):
    """Broker unreachable or protocol error (message never includes credentials)."""


def _encode_str(value: str) -> bytes:
    raw = value.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


def _remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        byte, n = n % 128, n // 128
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


def build_connect(
    client_id: str, keepalive: int = 30, username: str | None = None, password: str | None = None
) -> bytes:
    flags = 0x02  # clean session
    payload = _encode_str(client_id)
    if username:
        flags |= 0x80
        payload += _encode_str(username)
        if password:
            flags |= 0x40
            payload += _encode_str(password)
    var = _encode_str("MQTT") + bytes([4, flags]) + struct.pack("!H", keepalive)
    body = var + payload
    return bytes([0x10]) + _remaining_length(len(body)) + body


def build_subscribe(topic: str, packet_id: int = 1) -> bytes:
    body = struct.pack("!H", packet_id) + _encode_str(topic) + bytes([0])  # QoS 0
    return bytes([0x82]) + _remaining_length(len(body)) + body


DISCONNECT = b"\xe0\x00"


async def _read_packet(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    header = (await reader.readexactly(1))[0]
    mult, length = 1, 0
    for _ in range(4):
        byte = (await reader.readexactly(1))[0]
        length += (byte & 0x7F) * mult
        if not byte & 0x80:
            break
        mult *= 128
    else:
        raise MqttError("malformed MQTT remaining length")
    return header, await reader.readexactly(length) if length else b""


def parse_publish(header: int, body: bytes) -> tuple[str, bytes]:
    qos = (header >> 1) & 0x03
    (tlen,) = struct.unpack("!H", body[:2])
    topic = body[2 : 2 + tlen].decode("utf-8", "replace")
    offset = 2 + tlen + (2 if qos else 0)
    return topic, body[offset:]


def coerce(raw: bytes) -> Any:
    text = raw.decode("utf-8", "replace").strip()
    if text in ("true", "false"):
        return text == "true"
    if text == "":
        return None
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        pass
    if text[:1] in "{[":
        try:
            return json.loads(text)
        except ValueError:
            pass
    return text


async def read_retained(
    host: str,
    port: int,
    topic_prefix: str,
    *,
    timeout_s: float = 4.0,
    idle_s: float = 0.6,
    username: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    """Subscribe to `<topic_prefix>/#`, collect retained messages, disconnect."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout_s)
    except (TimeoutError, OSError) as exc:
        raise MqttError(f"cannot reach MQTT broker {host}:{port}: {type(exc).__name__}") from None
    fields: dict[str, Any] = {}
    try:
        writer.write(
            build_connect(
                f"tmcp-ro-{os.getpid()}-{id(fields) & 0xFFFF}", username=username, password=password
            )
        )
        await writer.drain()
        header, body = await asyncio.wait_for(
            _read_packet(reader), max(0.1, deadline - loop.time())
        )
        if header >> 4 != 2 or len(body) < 2 or body[1] != 0:
            raise MqttError(f"MQTT CONNACK refused (code {body[1] if len(body) > 1 else '?'})")
        writer.write(build_subscribe(f"{topic_prefix}/#"))
        await writer.drain()
        prefix = topic_prefix + "/"
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            wait = min(remaining, idle_s) if fields else remaining
            try:
                header, body = await asyncio.wait_for(_read_packet(reader), wait)
            except TimeoutError:
                break
            kind = header >> 4
            if kind == 9:  # SUBACK
                continue
            if kind == 3:  # PUBLISH (QoS 0 granted, so no PUBACK owed)
                topic, payload = parse_publish(header, body)
                if topic.startswith(prefix):
                    fields[topic[len(prefix) :]] = coerce(payload)
    except asyncio.IncompleteReadError:
        raise MqttError("MQTT broker closed the connection") from None
    finally:
        try:
            writer.write(DISCONNECT)
            await writer.drain()
        except Exception:  # pragma: no cover - best effort
            pass
        writer.close()
    return fields


def register_live_status_tool(
    mcp: MCPServer,
    *,
    host: str,
    port: int,
    namespace: str = "",
    timeout_s: float = 4.0,
    username: str | None = None,
    password: str | None = None,
) -> None:
    """Register `get_live_vehicle_status` (only called when MQTT_HOST is set)."""

    base = "teslamate" + (f"/{namespace.strip('/')}" if namespace.strip("/") else "")
    annotations = ToolAnnotations(
        read_only_hint=True,
        destructive_hint=False,
        idempotent_hint=True,
        open_world_hint=False,
    )
    description = (
        "Live vehicle status from TeslaMate's retained MQTT state: lock state, "
        "sentry mode, windows, doors, trunk/frunk, charging (plugged in, power, "
        "limit, time to full), climate, tire pressures and soft warnings, software "
        "update, geofence. Values are TeslaMate's LAST-KNOWN state: while the car "
        "is asleep or offline they reflect the moment it went to sleep (see "
        "`state`/`since` and `db_last_position_at`). This tool only reads the "
        "MQTT broker; it never contacts Tesla and cannot wake or command the car."
    )

    async def get_live_vehicle_status(
        ctx: Context,
        car_id: int = Field(default=1, ge=1, le=99, description="TeslaMate car id (usually 1)."),
    ) -> dict[str, Any]:
        fields = await read_retained(
            host,
            port,
            f"{base}/cars/{int(car_id)}",
            timeout_s=timeout_s,
            username=username,
            password=password,
        )
        if not fields:
            raise ValueError(
                f"No retained MQTT data for car {car_id}. Check the car id "
                "(get_basic_car_information) and that TeslaMate's MQTT is enabled."
            )
        summary = {k: fields[k] for k in SUMMARY_FIELDS if k in fields}
        freshness: dict[str, Any] = {}
        try:
            pool = ctx.request_context.lifespan_context.pool
            rows = await fetch_all(pool, _DB_FRESHNESS_SQL, {"car_id": int(car_id)})
            freshness = rows[0] if rows else {}
        except Exception:
            logger.warning("live status: DB freshness lookup failed", exc_info=False)
        return {
            "car_id": int(car_id),
            "source": "teslamate MQTT retained (last-known values)",
            "summary": summary,
            **freshness,
            "all_fields": dict(sorted(fields.items())),
        }

    get_live_vehicle_status.__annotations__["ctx"] = Context
    mcp.tool(name="get_live_vehicle_status", description=description, annotations=annotations)(
        get_live_vehicle_status
    )
