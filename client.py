"""TCP client (run on the phone, or another terminal on the laptop).

Reconnects automatically with exponential backoff and pings the server every 20s so a
dead connection (e.g. the phone switching from Wi-Fi to mobile data) is noticed quickly.

    python3 client.py --host 100.101.102.103 --port 5000 --id phone1

Type to chat:
    hello everyone      -> sent to all clients
    @laptop hi          -> sent only to the client with id "laptop"
    /list               -> show who is online
    /quit               -> exit
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import random
import sys
import threading
import time
import uuid

from protocol import ProtocolError, read_msg, send_msg

CONNECT_TIMEOUT = 10
HEARTBEAT_INTERVAL = 20  # send a ping if we have sent nothing for this long
READ_TIMEOUT = 60        # server answers pings, so silence this long means the link is dead
MAX_BACKOFF = 30


class Rejected(Exception):
    """The server refused us (bad token, bad id). Retrying will not help."""


class Quit(Exception):
    pass


def start_input_thread(loop: asyncio.AbstractEventLoop, inbox: asyncio.Queue) -> None:
    """Read stdin in a daemon thread so typing never blocks networking and Ctrl-C exits cleanly.
    Lines typed while disconnected stay queued and are sent after reconnecting."""

    def run() -> None:
        for line in sys.stdin:
            line = line.strip()
            if line:
                loop.call_soon_threadsafe(inbox.put_nowait, line)
        loop.call_soon_threadsafe(inbox.put_nowait, "/quit")  # stdin closed

    threading.Thread(target=run, daemon=True).start()


def to_message(line: str) -> dict:
    if line == "/quit":
        raise Quit
    if line == "/list":
        return {"type": "list"}
    if line.startswith("@"):
        to, _, body = line[1:].partition(" ")
        return {"type": "msg", "to": to, "body": body}
    return {"type": "msg", "to": "*", "body": line}


def show(msg: dict) -> None:
    kind = msg["type"]
    if kind == "msg":
        scope = "" if msg.get("to") == "*" else " (private)"
        print(f"[{msg.get('from')}]{scope} {msg.get('body')}")
    elif kind == "join":
        print(f"* {msg.get('id')} joined")
    elif kind == "leave":
        print(f"* {msg.get('id')} left")
    elif kind == "clients":
        print("* online:", ", ".join(msg.get("clients", [])))
    elif kind == "error":
        print("! server error:", msg.get("reason"))
    elif kind != "pong":
        print("?", msg)


async def receive_loop(reader: asyncio.StreamReader) -> None:
    while True:
        show(await asyncio.wait_for(read_msg(reader), READ_TIMEOUT))


async def send_loop(writer: asyncio.StreamWriter, inbox: asyncio.Queue) -> None:
    # The only task that writes to the socket, so heartbeats and chat never interleave.
    while True:
        try:
            line = await asyncio.wait_for(inbox.get(), HEARTBEAT_INTERVAL)
        except asyncio.TimeoutError:
            await send_msg(writer, {"type": "ping"})
            continue
        await send_msg(writer, to_message(line))


async def session(args: argparse.Namespace, inbox: asyncio.Queue) -> None:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(args.host, args.port), CONNECT_TIMEOUT
    )
    try:
        await send_msg(writer, {"type": "hello", "id": args.id, "token": args.token})
        welcome = await asyncio.wait_for(read_msg(reader), CONNECT_TIMEOUT)
        if welcome["type"] == "error":
            raise Rejected(welcome.get("reason"))
        print(f"* connected as {welcome['id']}; online: {', '.join(welcome.get('clients', []))}")

        tasks = [
            asyncio.create_task(receive_loop(reader)),
            asyncio.create_task(send_loop(writer, inbox)),
        ]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in pending:
            task.cancel()
        for task in done:
            task.result()  # re-raise whatever ended the session
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def main() -> None:
    parser = argparse.ArgumentParser(description="TCP chat client")
    parser.add_argument("--host", required=True, help="server IP or hostname")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--id", default=f"client-{uuid.uuid4().hex[:6]}",
                        help="unique name for this device")
    parser.add_argument("--token", default=os.environ.get("TCP_TOKEN", ""),
                        help="shared secret if the server sets TCP_TOKEN")
    args = parser.parse_args()

    inbox: asyncio.Queue[str] = asyncio.Queue()
    start_input_thread(asyncio.get_running_loop(), inbox)

    backoff = 1.0
    while True:
        started = time.monotonic()
        try:
            await session(args, inbox)
        except Quit:
            return
        except Rejected as e:
            print("! rejected by server:", e)
            return
        except (OSError, EOFError, asyncio.TimeoutError, ProtocolError) as e:
            print(f"! connection lost: {type(e).__name__}: {e}")

        if time.monotonic() - started > 30:
            backoff = 1.0  # we had a healthy session, so retry quickly
        delay = backoff + random.uniform(0, backoff / 2)  # jitter avoids reconnect stampedes
        print(f"* reconnecting in {delay:.1f}s")
        await asyncio.sleep(delay)
        backoff = min(backoff * 2, MAX_BACKOFF)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
