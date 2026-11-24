"""A client for edge-connect's normalized WebSocket (PROTOCOL.md)."""

import asyncio
import logging
import time
from typing import Awaitable, Callable, Optional

from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

# A book consumer needs the definitions, both book granularities and the prints; `quote` and
# `depth` would only cost parsing.
KINDS = ("instrument", "book", "order_book", "trade")
HEARTBEAT_SECONDS = 20.0
# An order-level re-baseline of a deep market is one frame of several MB; Hummingbot's default cap is 4 MB.
MAX_FRAME_BYTES = 64 * 1024 * 1024
BACKOFF_MIN_SECONDS = 0.5
BACKOFF_MAX_SECONDS = 30.0
STABLE_SESSION_SECONDS = 30.0

logger = logging.getLogger(__name__)


def subscriptions(source_name: str, symbol: str) -> list:
    return [{"source_name": source_name, "symbol": symbol, "type": kind} for kind in KINDS]


class EdgeConnectClient:
    """One connection to edge-connect, reconnected with back-off for as long as `run` runs."""

    def __init__(self, url: str, api_factory: WebAssistantsFactory):
        self.url = url
        self._api_factory = api_factory
        self._ws: Optional[WSAssistant] = None

    @property
    def connected(self) -> bool:
        return self._ws is not None

    async def run(self, on_connect: Callable[[], Awaitable[None]], on_message: Callable[[dict], None]):
        delay = BACKOFF_MIN_SECONDS
        while True:
            started = time.monotonic()
            ws = await self._api_factory.get_ws_assistant()
            try:
                await ws.connect(ws_url=self.url, ping_timeout=HEARTBEAT_SECONDS, max_msg_size=MAX_FRAME_BYTES)
                self._ws = ws
                await on_connect()
                async for response in ws.iter_messages():
                    if response is not None and isinstance(response.data, dict):
                        on_message(response.data)
                logger.warning(f"edge-connect at {self.url} closed the connection")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"edge-connect at {self.url}: {e!r}")
            finally:
                self._ws = None
                await ws.disconnect()
            if time.monotonic() - started >= STABLE_SESSION_SECONDS:
                delay = BACKOFF_MIN_SECONDS
            await asyncio.sleep(delay)
            delay = min(delay * 2, BACKOFF_MAX_SECONDS)

    async def send(self, method: str, subscription: dict):
        if self._ws is not None:
            await self._ws.send(WSJSONRequest(payload={"method": method, "subscription": subscription}))
