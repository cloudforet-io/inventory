import logging

from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class RepositoryManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repo_connector: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="repository"
        )

    def get_plugin(self, plugin_id: str) -> dict:
        return self.repo_connector.dispatch("Plugin.get", {"plugin_id": plugin_id})
