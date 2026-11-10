"""get_live_vehicle_status: opt-in, read-only MQTT retained-state reader."""

from __future__ import annotations

import asyncio
import struct

import pytest

from teslamate_mcp.config import Settings
from teslamate_mcp.server import create_server
from teslamate_mcp.tools.live_status import (
    MqttError,
    _read_packet,
    _remaining_length,
    coerce,
    read_retained,
)

_DUMMY_DB_URL = "postgresql://teslamate:secret@example.test/teslamate"


def _publish(topic: str, payload: str, retain: bool = True) -> bytes:
    t = topic.encode()
    body = struct.pack("!H", len(t)) + t + payload.encode()
    return bytes([0x30 | (1 if retain else 0)]) + _remaining_length(len(body)) + body


class FakeBroker:
    def __init__(self, retained: dict[str, str], connack_code: int = 0) -> None:
        self.retained = retained
        self.connack_code = connack_code
        self.client_packet_types: list[int] = []
        self.subscriptions: list[str] = []

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                header, body = await _read_packet(reader)
                kind = header >> 4
                self.client_packet_types.append(kind)
                if kind == 1:
                    writer.write(bytes([0x20, 2, 0, self.connack_code]))
                elif kind == 8:
                    pid = body[:2]
                    (tlen,) = struct.unpack("!H", body[2:4])
                    self.subscriptions.append(body[4 : 4 + tlen].decode())
                    writer.write(bytes([0x90, 3]) + pid + b"\x00")
                    for topic, payload in self.retained.items():
                        writer.write(_publish(topic, payload))
                elif kind == 14:
                    break
                await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()

    async def __aenter__(self) -> int:
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    async def __aexit__(self, *exc: object) -> None:
        self.server.close()
        await self.server.wait_closed()


RETAINED = {
    "teslamate/cars/1/locked": "true",
    "teslamate/cars/1/sentry_mode": "false",
    "teslamate/cars/1/windows_open": "false",
    "teslamate/cars/1/tpms_pressure_fl": "2.9",
    "teslamate/cars/1/battery_level": "32",
    "teslamate/cars/1/state": "asleep",
    "teslamate/cars/1/active_route": '{"error":"No active route available"}',
    "teslamate/cars/2/locked": "false",
}


@pytest.mark.asyncio
async def test_reads_retained_and_never_publishes() -> None:
    broker = FakeBroker(RETAINED)
    async with broker as port:
        fields = await read_retained("127.0.0.1", port, "teslamate/cars/1", timeout_s=3)
    assert fields["locked"] is True
    assert fields["sentry_mode"] is False
    assert fields["tpms_pressure_fl"] == 2.9
    assert fields["battery_level"] == 32
    assert fields["state"] == "asleep"
    assert fields["active_route"] == {"error": "No active route available"}
    assert "cars/2" not in str(fields) and len(fields) == 7
    assert broker.subscriptions == ["teslamate/cars/1/#"]
    # Only CONNECT(1), SUBSCRIBE(8), DISCONNECT(14) — never PUBLISH(3).
    assert set(broker.client_packet_types) <= {1, 8, 14}
    assert 3 not in broker.client_packet_types


@pytest.mark.asyncio
async def test_connack_refused_raises() -> None:
    async with FakeBroker(RETAINED, connack_code=5) as port:
        with pytest.raises(MqttError, match="refused"):
            await read_retained("127.0.0.1", port, "teslamate/cars/1", timeout_s=2)


@pytest.mark.asyncio
async def test_unreachable_broker_raises() -> None:
    with pytest.raises(MqttError, match="cannot reach"):
        await read_retained("127.0.0.1", 1, "teslamate/cars/1", timeout_s=1)


def test_coerce() -> None:
    assert coerce(b"true") is True and coerce(b"false") is False
    assert coerce(b"12") == 12 and coerce(b"1.5") == 1.5
    assert coerce(b"") is None and coerce(b"online") == "online"


@pytest.mark.asyncio
async def test_tool_absent_by_default() -> None:
    mcp = create_server(Settings(database_url=_DUMMY_DB_URL))  # type: ignore[call-arg]
    assert "get_live_vehicle_status" not in {t.name for t in await mcp.list_tools()}


@pytest.mark.asyncio
async def test_tool_registered_read_only_when_mqtt_host_set() -> None:
    mcp = create_server(Settings(database_url=_DUMMY_DB_URL, mqtt_host="mosquitto"))  # type: ignore[call-arg]
    tools = {t.name: t for t in await mcp.list_tools()}
    tool = tools["get_live_vehicle_status"]
    assert tool.annotations.read_only_hint is True
    assert tool.annotations.destructive_hint is False
    props = tool.input_schema.get("properties", {})
    assert set(props) == {"car_id"}
    assert props["car_id"]["minimum"] == 1 and props["car_id"]["maximum"] == 99
    assert "cannot wake" in tool.description
