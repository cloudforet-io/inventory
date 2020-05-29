import logging

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.inventory.lib import rule_matcher
from spaceone.inventory.model.network_model import Network
from spaceone.inventory.lib.resource_manager import ResourceManager

_LOGGER = logging.getLogger(__name__)


class NetworkManager(BaseManager, ResourceManager):

    resource_keys = ['network_id']
    query_method = 'list_networks'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.network_model: Network = self.locator.get_model('Network')

    def create_network(self, params):
        def _rollback(network_vo):
            _LOGGER.info(f'[ROLLBACK] Delete network : {network_vo.network_id}')
            network_vo.delete()

        network_vo: Network = self.network_model.create(params)
        self.transaction.add_rollback(_rollback, network_vo)

        return network_vo

    def update_network(self, params):
        return self.update_network_by_vo(params,
                                         self.get_network(params.get('network_id'), params.get('domain_id')))

    def update_network_by_vo(self, params, network_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("network_id")}')
            network_vo.update(old_data)

        self.transaction.add_rollback(_rollback, network_vo.to_dict())

        return network_vo.update(params)

    def delete_network(self, network_id, domain_id):
        self.delete_network_vo(self.get_network(network_id, domain_id))

    def get_network(self, network_id, domain_id, only=None):
        return self.network_model.get(network_id=network_id, domain_id=domain_id, only=only)

    def list_networks(self, query):
        return self.network_model.query(**query)

    def stat_networks(self, query):
        return self.network_model.stat(**query)

    @staticmethod
    def delete_network_vo(network_vo):
        network_vo.delete()
