"""An in-process edge-connect WebSocket for tests."""

import json

from aiohttp import web


class StubEdgeConnect:
    """Replays `catalog` on connect and, scoped, on each subscribe; answers every control frame once.

    That is PROTOCOL.md's lifecycle, down to edge-connect's cap on subscriptions per client.
    """

    def __init__(self, catalog=(), max_subscriptions=256):
        self.catalog = list(catalog)
        self.max_subscriptions = max_subscriptions
        self.received = []
        self.connections = 0
        self._sockets = []

    async def start(self, port=0):
        app = web.Application()
        app.router.add_get("/", self._serve)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", port)
        await site.start()
        self.url = f"ws://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/"

    async def stop(self):
        await self._runner.cleanup()

    async def push(self, *frames):
        for ws in self._sockets:
            try:
                for frame in frames:
                    await ws.send_json(frame)
            except ConnectionResetError:
                pass  # a connection the client has already closed


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
        subscriptions = []
        async for message in ws:
            frame = json.loads(message.data)
            self.received.append(frame)
            method, subscription = frame.get("method"), frame.get("subscription")
            if method == "subscribe" and len(subscriptions) >= self.max_subscriptions:
                await ws.send_json({"channel": "error", "error": "max subscriptions reached"})
            elif method in ("subscribe", "unsubscribe"):
                if method == "subscribe":
                    subscriptions.append(subscription)
                elif subscription in subscriptions:
                    subscriptions.remove(subscription)
                await ws.send_json({"channel": "subscription_response", "method": method,
                                    "subscription": subscription})
                if method == "subscribe":
                    for replayed in self.catalog:
                        if (replayed["type"], replayed["symbol"]) == (subscription.get("type"), subscription.get("symbol")):
                            await ws.send_json(replayed)
            else:
                await ws.send_json({"channel": "error", "error": "unrecognized message"})
        return ws
