"""DataUpdateCoordinator for Kostal KSEM — push-based via WebSocket."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

import aiohttp
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import KostalKSEM, authenticate
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class KostalCoordinator(DataUpdateCoordinator[Dict[str, Any]]):
    """Manages the KostalKSEM client and distributes data to HA entities."""

    def __init__(self, hass: HomeAssistant, host: str,
                 username: str, password: str) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=None,  # push-only; no polling
        )
        self._host = host
        self._username = username
        self._password = password
        self._ksem = KostalKSEM(host, username, password)
        self._session: aiohttp.ClientSession | None = None
        self._first_data = asyncio.Event()

    async def async_start(self) -> None:
        """Authenticate, start WebSocket connections, wait for first data."""
        self._session = aiohttp.ClientSession()
        self._ksem.on_update(self._on_ws_update)
        try:
            await self._ksem.start(self._session)
        except aiohttp.ClientResponseError as err:
            await self._session.close()
            raise UpdateFailed(f"Auth failed: {err}") from err
        except Exception as err:
            await self._session.close()
            raise UpdateFailed(f"Connection failed: {err}") from err

        try:
            await asyncio.wait_for(self._first_data.wait(), timeout=15)
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout waiting for first WS data — proceeding with partial data")

        self.async_set_updated_data(self._ksem.get_all_data())

    async def async_stop(self) -> None:
        """Stop all WebSocket connections and close the HTTP session."""
        await self._ksem.stop()
        if self._session:
            await self._session.close()
            self._session = None

    async def _async_update_data(self) -> Dict[str, Any]:
        """Return the current snapshot (called on manual refresh)."""
        return self._ksem.get_all_data()

    @callback
    def _on_ws_update(self, channel: str, device_id: str, gdr: Any) -> None:
        """Called from the WS subscriber task on every new GDR message."""
        self._first_data.set()
        self.async_set_updated_data(self._ksem.get_all_data())
