"""Multi-client TCP server (run on the laptop).

Every client connects, introduces itself with a "hello" carrying its id, and can then
send messages to one other client or to everyone. Uses asyncio, so a single process
handles many clients at once without threads.

    python3 server.py --port 5000
    TCP_TOKEN=secret python3 server.py      # require clients to send this token
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hmac
import logging
import os
import re

from protocol import ProtocolError, read_msg, send_msg

HELLO_TIMEOUT = 10   # seconds a new connection has to send its hello
IDLE_TIMEOUT = 60    # drop a client we hear nothing from (clients ping every 20s)
OUTBOX_SIZE = 100    # queued messages per client before we treat it as stuck
CLIENT_ID = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

log = logging.getLogger("server")


class Connection:
    """One connected client. Outgoing messages go through a queue drained by a single task,
    so a slow client never blocks the others and writes to one socket never interleave."""

    def __init__(self, client_id: str, writer: asyncio.StreamWriter):
        self.id = client_id
        self.writer = writer
        self.peer = writer.get_extra_info("peername")
        self.outbox: asyncio.Queue[dict] = asyncio.Queue(OUTBOX_SIZE)

    def send(self, msg: dict) -> None:
        try:
            self.outbox.put_nowait(msg)
        except asyncio.QueueFull:
            log.warning("%s is not reading its messages; disconnecting it", self.id)
            self.writer.close()

    async def pump(self) -> None:
        try:
            while True:
                await send_msg(self.writer, await self.outbox.get())
        except (ConnectionError, OSError):
            self.writer.close()  # the read loop in Server.handle notices and cleans up


class Server:
    def __init__(self, token: str | None):
        self.token = token
        self.clients: dict[str, Connection] = {}

    def check_hello(self, msg: dict) -> str | None:
        """Return an error reason, or None if the hello is acceptable."""
        if msg["type"] != "hello":
            return "first message must be hello"
        client_id = msg.get("id")
        if not isinstance(client_id, str) or not CLIENT_ID.match(client_id):
            return "id must be 1-32 characters of letters, digits, '_' or '-'"
        if self.token is not None:
            given = msg.get("token")
            if not isinstance(given, str) or not hmac.compare_digest(given, self.token):
                return "bad token"
        return None

    def broadcast(self, msg: dict, exclude: str | None = None) -> None:
        for conn in list(self.clients.values()):
            if conn.id != exclude:
                conn.send(msg)

    def dispatch(self, conn: Connection, msg: dict) -> None:
        kind = msg["type"]
        if kind == "ping":
            conn.send({"type": "pong"})
        elif kind == "list":
            conn.send({"type": "clients", "clients": sorted(self.clients)})
        elif kind == "msg":
            to, body = msg.get("to"), msg.get("body")
            if not isinstance(to, str) or not isinstance(body, str):
                conn.send({"type": "error", "reason": "msg needs string 'to' and 'body'"})
                return
            out = {"type": "msg", "from": conn.id, "to": to, "body": body}
            if to == "*":
                self.broadcast(out, exclude=conn.id)
            elif to in self.clients:
                self.clients[to].send(out)
            else:
                conn.send({"type": "error", "reason": f"no client named {to!r}"})
        else:
            conn.send({"type": "error", "reason": f"unknown type {kind!r}"})

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        conn: Connection | None = None
        pump: asyncio.Task | None = None
        try:
            hello = await asyncio.wait_for(read_msg(reader), HELLO_TIMEOUT)
            error = self.check_hello(hello)
            if error:
                log.info("rejected %s: %s", peer, error)
                await send_msg(writer, {"type": "error", "reason": error})
                return

            conn = Connection(hello["id"], writer)
            old = self.clients.get(conn.id)
            if old:
                # Mobile clients often reconnect before the old socket is noticed as dead.
                log.info("%s reconnected from %s; closing its old connection", conn.id, peer)
                old.writer.close()
            self.clients[conn.id] = conn
            pump = asyncio.create_task(conn.pump())
            log.info("%s connected from %s (%d online)", conn.id, peer, len(self.clients))

            conn.send({"type": "welcome", "id": conn.id, "clients": sorted(self.clients)})
            if not old:
                self.broadcast({"type": "join", "id": conn.id}, exclude=conn.id)

            while True:
                msg = await asyncio.wait_for(read_msg(reader), IDLE_TIMEOUT)
                self.dispatch(conn, msg)
        except asyncio.TimeoutError:
            log.info("%s timed out", conn.id if conn else peer)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            log.info("%s disconnected", conn.id if conn else peer)
        except ProtocolError as e:
            log.warning("%s sent bad data: %s", conn.id if conn else peer, e)
        finally:
            # Only remove the entry if it is still ours; a reconnect may have replaced it.
            if conn and self.clients.get(conn.id) is conn:
                del self.clients[conn.id]
                self.broadcast({"type": "leave", "id": conn.id})
                log.info("%d online", len(self.clients))
            if pump:
                pump.cancel()
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-client TCP server")
    parser.add_argument("--host", default="0.0.0.0", help="interface to listen on (default: all)")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    token = os.environ.get("TCP_TOKEN") or None
    server = Server(token)
    tcp = await asyncio.start_server(server.handle, args.host, args.port)
    for sock in tcp.sockets:
        log.info("listening on %s", sock.getsockname())
    if token is None:
        log.warning("no TCP_TOKEN set: anyone who can reach this port can connect")
    async with tcp:
        await tcp.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
