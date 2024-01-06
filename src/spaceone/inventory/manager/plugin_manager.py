import logging
from typing import Tuple

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

__ALL__ = ["PluginManager"]

_LOGGER = logging.getLogger(__name__)


class PluginManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.plugin_connector: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="plugin"
        )

    def get_endpoint(
        self,
        plugin_id: str,
        domain_id: str,
        upgrade_mode: str = "AUTO",
        version: str = None,
    ) -> Tuple[str, str]:
        system_token = config.get_global("TOKEN")

        response = self.plugin_connector.dispatch(
            "Plugin.get_plugin_endpoint",
            {
                "plugin_id": plugin_id,
                "domain_id": domain_id,
                "upgrade_mode": upgrade_mode,
                "version": version,
            },
            token=system_token,
        )

        return response.get("endpoint"), response.get("updated_version")
