import logging

from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.inventory.connector.config_connector import ConfigConnector

_LOGGER = logging.getLogger(__name__)


class ConfigManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config_conn: SpaceConnector = self.locator.get_connector('SpaceConnector', service='config')

    def get_domain_config(self, name, domain_id):
        result = self.config_conn.dispatch('DomainConfig.get', {'name': name, 'domain_id': domain_id})

        rules = result['data']['rules']
        policies = {}
        for rule in rules:
            policies[rule['k']] = int(rule['v'])
        _LOGGER.debug(f'[get_domain_config] policies : {policies}')
        return policies
