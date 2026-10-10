"""Tiny room relay for Placement Day. Run:  pip install websockets  &&  python relay.py
Players in the same room code see each other, chat, and get a live results feed.
Env: PORT (default 8765)."""
import asyncio
import json
import os
import random
import string

import websockets

ROOMS = {}  # room code -> {websocket: info}
MAX_PLAYERS = 8
FORWARD = {"pos", "chat", "ev"}
NUM_CHARS = 13


async def send(ws, obj):
    try:
        await ws.send(json.dumps(obj))
    except Exception:
        pass


async def broadcast(room, obj, skip=None):
    for w in list(ROOMS.get(room, {})):
        if w is not skip:
            await send(w, obj)


async def handler(ws):
    info = {"id": "".join(random.choices(string.ascii_lowercase, k=6)), "room": None, "name": "PLAYER", "ci": 0}
    try:
        async for raw in ws:
            if len(raw) > 2000:
                continue
            try:
                m = json.loads(raw)
            except ValueError:
                continue
            t = m.get("t")
            if t == "join" and info["room"] is None:
                room = str(m.get("room", "PIET"))[:12].upper() or "PIET"
                members = ROOMS.setdefault(room, {})
                if len(members) >= MAX_PLAYERS:
                    await send(ws, {"t": "full"})
                    continue
                info["room"] = room
                info["name"] = str(m.get("name", "PLAYER"))[:14] or "PLAYER"
                try:
                    want = int(m.get("ci", 0))
                except (TypeError, ValueError):
                    want = 0
                taken = {i["ci"] for i in members.values()}
                if want in taken or not 0 <= want < NUM_CHARS:
                    free = [c for c in range(NUM_CHARS) if c not in taken]
                    want = free[0] if free else 0
                info["ci"] = want
                peers = [{"id": i["id"], "name": i["name"], "ci": i["ci"]} for i in members.values()]
                host = next(iter(members.values()))["id"] if members else info["id"]
                members[ws] = info
                await send(ws, {"t": "welcome", "id": info["id"], "host": host, "ci": info["ci"], "peers": peers})
                await broadcast(room, {"t": "join", "id": info["id"], "name": info["name"], "ci": info["ci"]}, skip=ws)
            elif t == "msg" and info["room"]:
                m["from"] = info["id"]
                to = m.get("to", "*")
                for w, i in list(ROOMS.get(info["room"], {}).items()):
                    if w is ws:
                        continue
                    if to == "*" or i["id"] == to:
                        await send(w, m)
            elif t in FORWARD and info["room"]:
                m["id"] = info["id"]
                await broadcast(info["room"], m, skip=ws)
    except websockets.ConnectionClosed:
        pass
    finally:
        room = info["room"]
        if room and room in ROOMS:
            members = ROOMS[room]
            was_host = bool(members) and next(iter(members.values()))["id"] == info["id"]
            members.pop(ws, None)
            if was_host:
                await broadcast(room, {"t": "closed"})
                ROOMS.pop(room, None)
            else:
                await broadcast(room, {"t": "leave", "id": info["id"]})
                if not members:
                    ROOMS.pop(room, None)


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with websockets.serve(handler, "0.0.0.0", port):
        print("Placement Day relay listening on port", port)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
