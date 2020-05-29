import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.error import *
from spaceone.inventory.connector.repository_connector import RepositoryConnector

_LOGGER = logging.getLogger(__name__)


class RepositoryManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repo_connector: RepositoryConnector = self.locator.get_connector('RepositoryConnector')

    def get_plugin(self, plugin_id, domain_id):
        return self.repo_connector.get_plugin(plugin_id, domain_id)

    def check_plugin_version(self, plugin_id, version, domain_id):
        versions = self.repo_connector.get_plugin_versions(plugin_id, domain_id)

        if version not in versions:
            raise ERROR_INVALID_PLUGIN_VERSION(plugin_id=plugin_id, version=version)

