import logging
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

__ALL__ = ['PluginManager']

_LOGGER = logging.getLogger(__name__)


class PluginManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.plugin_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', service='plugin')

    def get_endpoint(self, plugin_id, version, domain_id, upgrade_mode='AUTO'):
        response = self.plugin_connector.dispatch('Plugin.get_plugin_endpoint', {
            'plugin_id': plugin_id,
            'version': version,
            'upgrade_mode': upgrade_mode,
            'domain_id': domain_id
        })

        return response.get('endpoint'), response.get('updated_version')
