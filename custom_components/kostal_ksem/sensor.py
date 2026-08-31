"""Sensor platform for Kostal KSEM."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import KostalCoordinator

_LOGGER = logging.getLogger(__name__)

# Device classes that HA's SensorDeviceClass supports
_HA_DEVICE_CLASSES = {dc.value for dc in SensorDeviceClass}

# Unit strings → state class override (energy counters are always increasing)
_TOTAL_INCREASING_UNITS = {"kWh", "kvarh", "kVAh"}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: KostalCoordinator = hass.data[DOMAIN][entry.entry_id]

    known_unique_ids: set[str] = set()

    @callback
    def _add_new_sensors() -> None:
        new_entities = _build_entities(coordinator, entry, known_unique_ids)
        if new_entities:
            async_add_entities(new_entities)

    # Add sensors from initial data snapshot
    _add_new_sensors()

    # Re-check on every coordinator update (handles late-arriving devices)
    coordinator.async_add_listener(_add_new_sensors)


def _build_entities(
    coordinator: KostalCoordinator,
    entry: ConfigEntry,
    known_ids: set[str],
) -> list[KostalSensor]:
    entities: list[KostalSensor] = []
    data = coordinator.data or {}

    for channel_key, devices in data.items():
        for device_id, device_data in devices.items():
            for sensor_name, sensor_info in device_data.get("sensors", {}).items():
                uid = f"{entry.entry_id}_{device_id}_{sensor_name}"
                if uid in known_ids:
                    continue
                known_ids.add(uid)
                entities.append(
                    KostalSensor(
                        coordinator=coordinator,
                        entry_id=entry.entry_id,
                        channel_key=channel_key,
                        device_id=device_id,
                        device_label=device_data.get("label", device_id),
                        sensor_name=sensor_name,
                        sensor_info=sensor_info,
                    )
                )

    return entities


class KostalSensor(CoordinatorEntity[KostalCoordinator], SensorEntity):
    """A single sensor entity mirroring one value from the Kostal KSEM API."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: KostalCoordinator,
        entry_id: str,
        channel_key: str,
        device_id: str,
        device_label: str,
        sensor_name: str,
        sensor_info: dict[str, Any],
    ) -> None:
        super().__init__(coordinator)
        self._channel_key = channel_key
        self._device_id = device_id
        self._sensor_name = sensor_name

        self._attr_unique_id = f"{entry_id}_{device_id}_{sensor_name}"
        self._attr_name = sensor_info.get("description") or sensor_name

        unit = sensor_info.get("unit") or None
        self._attr_native_unit_of_measurement = unit

        dc_str = sensor_info.get("device_class", "")
        if dc_str and dc_str in _HA_DEVICE_CLASSES:
            self._attr_device_class = SensorDeviceClass(dc_str)

        if unit in _TOTAL_INCREASING_UNITS:
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif dc_str or unit:
            self._attr_state_class = SensorStateClass.MEASUREMENT

        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, device_id)},
            name=device_label,
            manufacturer="Kostal",
        )

    @property
    def native_value(self) -> Any:
        data = self.coordinator.data or {}
        sensor = (
            data
            .get(self._channel_key, {})
            .get(self._device_id, {})
            .get("sensors", {})
            .get(self._sensor_name, {})
        )
        return sensor.get("value")

    @property
    def available(self) -> bool:
        return self.coordinator.last_update_success and self.native_value is not None
