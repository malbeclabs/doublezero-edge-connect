"""An in-process edge-connect WebSocket for tests."""

import json

from aiohttp import web


class StubEdgeConnect:
    """Replays `catalog` on connect, acknowledges control frames and records them, as PROTOCOL.md describes."""

    def __init__(self, catalog=()):
        self.catalog = list(catalog)
        self.received = []
        self.connections = 0
        self._sockets = []

    async def start(self):
        app = web.Application()
        app.router.add_get("/", self._serve)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"ws://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/"

    async def stop(self):
        await self._runner.cleanup()

    async def push(self, *frames):
        for ws in self._sockets:
            for frame in frames:
                await ws.send_json(frame)

    async def drop(self):
        for ws in self._sockets:
            await ws.close()
        self._sockets.clear()

    async def _serve(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.connections += 1
        for frame in self.catalog:
            await ws.send_json(frame)
        self._sockets.append(ws)
        async for message in ws:
            frame = json.loads(message.data)
            self.received.append(frame)
            if frame.get("method") in ("subscribe", "unsubscribe"):
                await ws.send_json({"channel": "subscription_response", "method": frame["method"],
                                    "subscription": frame["subscription"]})
            else:
                await ws.send_json({"channel": "error", "error": "unrecognized message"})
        return ws
