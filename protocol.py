"""Wire format shared by the server and the client.

TCP delivers a stream of bytes, not separate messages, so each message is framed as:

    [4-byte big-endian length][that many bytes of UTF-8 JSON]

The reader first reads exactly 4 bytes to learn the length, then reads exactly that many bytes.
Every message is a JSON object with a "type" field.
"""
from __future__ import annotations

import asyncio
import json
import struct

HEADER = struct.Struct("!I")  # unsigned 32-bit int, network byte order
MAX_MESSAGE_BYTES = 1 << 20   # 1 MiB; protects against garbage lengths eating memory


class ProtocolError(Exception):
    """The peer sent something that does not follow the wire format."""


def encode(msg: dict) -> bytes:
    data = json.dumps(msg, separators=(",", ":")).encode("utf-8")
    if len(data) > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"message too large ({len(data)} bytes)")
    return HEADER.pack(len(data)) + data


async def send_msg(writer: asyncio.StreamWriter, msg: dict) -> None:
    writer.write(encode(msg))
    await writer.drain()


async def read_msg(reader: asyncio.StreamReader) -> dict:
    """Read one message. Raises asyncio.IncompleteReadError if the peer closes the connection."""
    header = await reader.readexactly(HEADER.size)
    (length,) = HEADER.unpack(header)
    if length > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"declared length {length} exceeds limit")
    data = await reader.readexactly(length)
    try:
        msg = json.loads(data)
    except ValueError as e:
        raise ProtocolError("invalid JSON") from e
    if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
        raise ProtocolError("message must be a JSON object with a string 'type'")
    return msg
