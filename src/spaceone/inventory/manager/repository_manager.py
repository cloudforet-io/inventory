import logging

from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)


class RepositoryManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repo_connector: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="repository"
        )

    def get_plugin(self, plugin_id: str) -> dict:
        return self.repo_connector.dispatch("Plugin.get", {"plugin_id": plugin_id})

    def check_plugin_version(self, plugin_id: str, version: str) -> None:
        response = self.repo_connector.dispatch(
            "Plugin.get_versions", {"plugin_id": plugin_id}
        )

        if version not in response.get("results", []):
            raise ERROR_INVALID_PLUGIN_VERSION(plugin_id=plugin_id, version=version)
