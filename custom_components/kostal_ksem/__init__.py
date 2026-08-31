"""Kostal KSEM Home Assistant integration."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.update_coordinator import UpdateFailed

from .const import CONF_HOST, CONF_PASSWORD, CONF_USERNAME, DEFAULT_USERNAME, DOMAIN
from .coordinator import KostalCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    coordinator = KostalCoordinator(
        hass=hass,
        host=entry.data[CONF_HOST],
        username=entry.data.get(CONF_USERNAME, DEFAULT_USERNAME),
        password=entry.data[CONF_PASSWORD],
    )

    try:
        await coordinator.async_start()
    except UpdateFailed as err:
        raise ConfigEntryNotReady(str(err)) from err

    hass.data[DOMAIN][entry.entry_id] = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator: KostalCoordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_stop()
    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload entry when options change (e.g. after reconfigure)."""
    await hass.config_entries.async_reload(entry.entry_id)
