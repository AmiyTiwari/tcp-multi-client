# tcp-multi-client

A small multi-client TCP chat in plain Python 3 (no dependencies). The laptop runs the server;
phones and other devices run the client.

| File | What it does |
|---|---|
| `protocol.py` | Message framing: 4-byte length + JSON. Used by both sides. |
| `server.py` | asyncio server. Many clients, broadcast + private messages, token auth, idle timeout. |
| `client.py` | Client with automatic reconnect (exponential backoff + jitter) and 20s heartbeat. |

## Run it

```bash
# laptop
TCP_TOKEN=pick-a-secret python3 server.py --port 5000

# any client (another terminal, or the phone)
TCP_TOKEN=pick-a-secret python3 client.py --host <SERVER_IP> --port 5000 --id phone1
```

In the client: `hello` goes to everyone, `@laptop hi` goes to one client, `/list` shows who is
online, `/quit` exits.

Which `<SERVER_IP>` to use:

| Where the devices are | `--host` |
|---|---|
| Same machine | `127.0.0.1` |
| Same Wi-Fi | Laptop's LAN IP: `ipconfig getifaddr en0` |
| Different cities | Laptop's Tailscale IP (`tailscale ip -4`), or the host:port of a TCP tunnel (ngrok `tcp`, Pinggy, frp) |

macOS will ask whether to allow incoming connections for Python the first time. Allow it, or
devices other than the laptop itself can't connect.

## Running the client on a phone

- **Android:** Termux (`pkg install python`) or Pydroid 3. Copy `client.py` and `protocol.py`.
- **iOS:** a-Shell or Pythonista. Copy the same two files.

## Wire protocol

Every frame is `[uint32 big-endian length][UTF-8 JSON object]`, max 1 MiB.

| Direction | Message |
|---|---|
| client → server (first) | `{"type":"hello","id":"phone1","token":"..."}` |
| server → client | `{"type":"welcome","id":"phone1","clients":[...]}` or `{"type":"error","reason":...}` |
| client → server | `{"type":"msg","to":"<id>" or "*","body":"..."}`, `{"type":"list"}`, `{"type":"ping"}` |
| server → client | `{"type":"msg","from":...,"to":...,"body":...}`, `join`, `leave`, `clients`, `pong`, `error` |

Any app in any language can be a client by following this format.

## Behaviour worth knowing

- **Same id reconnecting** replaces the old connection. This handles phones whose old socket
  hasn't been detected as dead yet.
- **Dead connections**: the server drops clients silent for 60s. Clients ping every 20s and
  reconnect if they hear nothing for 60s.
- **Slow clients**: each client has a 100-message outbox. A client that falls further behind is
  disconnected, so it can't stall everyone else.
- **No offline delivery**: messages to clients that aren't connected are dropped. A message in
  flight when a connection breaks can also be lost. Add storage plus message ids and acks if you
  need guaranteed delivery.
- **No encryption**: traffic is plain text. Over Tailscale it is already encrypted. Over a
  public tunnel or open port, wrap the sockets in TLS (`ssl` module) before sending anything private.
