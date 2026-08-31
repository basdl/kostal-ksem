"""Config flow for Kostal KSEM integration."""
from __future__ import annotations

import logging
from typing import Any

import aiohttp
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.exceptions import HomeAssistantError

from .api import authenticate
from .const import CONF_HOST, CONF_PASSWORD, CONF_USERNAME, DEFAULT_HOST, DEFAULT_USERNAME, DOMAIN

_LOGGER = logging.getLogger(__name__)

STEP_USER_SCHEMA = vol.Schema({
    vol.Required(CONF_HOST, default=DEFAULT_HOST): str,
    vol.Optional(CONF_USERNAME, default=DEFAULT_USERNAME): str,
    vol.Required(CONF_PASSWORD): str,
})


async def _validate_input(hass: HomeAssistant, data: dict[str, Any]) -> str:
    """Try to authenticate and return the device host as the title."""
    async with aiohttp.ClientSession() as session:
        try:
            await authenticate(
                session,
                data[CONF_HOST],
                data[CONF_USERNAME],
                data[CONF_PASSWORD],
            )
        except aiohttp.ClientResponseError as err:
            if err.status in (400, 401):
                raise InvalidAuth from err
            raise CannotConnect from err
        except Exception as err:
            raise CannotConnect from err
    return data[CONF_HOST]


class KostalKSEMConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle the config flow for Kostal KSEM."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                title = await _validate_input(self.hass, user_input)
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error during config flow")
                errors["base"] = "unknown"
            else:
                await self.async_set_unique_id(
                    f"{user_input[CONF_HOST]}_{user_input[CONF_USERNAME]}"
                )
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"Kostal KSEM ({title})",
                    data=user_input,
                )

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_SCHEMA,
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Allow updating credentials without removing the entry."""
        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                await _validate_input(self.hass, user_input)
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error during reconfigure")
                errors["base"] = "unknown"
            else:
                return self.async_update_reload_and_abort(
                    self._get_reconfigure_entry(),
                    data_updates=user_input,
                )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=STEP_USER_SCHEMA,
            errors=errors,
        )


class CannotConnect(HomeAssistantError):
    pass


class InvalidAuth(HomeAssistantError):
    pass
