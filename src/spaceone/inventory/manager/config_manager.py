import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.connector.config_connector import ConfigConnector

_LOGGER = logging.getLogger(__name__)

class ConfigManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config_conn: ConfigConnector = self.locator.get_connector('ConfigConnector')

    def get_domain_config(self, name, domain_id):
        result = self.config_conn.get_domain_config(name, domain_id)
        rules = result['data']['rules']
        policies = {}
        for rule in rules:
            policies[rule['k']] = int(rule['v'])
        _LOGGER.debug(f'[get_domain_config] policies : {policies}')
        return policies
