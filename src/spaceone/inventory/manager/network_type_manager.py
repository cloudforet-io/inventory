import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.network_type_model import NetworkType

_LOGGER = logging.getLogger(__name__)


class NetworkTypeManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ntype_model: NetworkType = self.locator.get_model('NetworkType')

    def create_network_type(self, params):
        def _rollback(ntype_vo):
            _LOGGER.info(f'[ROLLBACK] Delete network type : {ntype_vo.name} ({ntype_vo.network_type_id})')
            ntype_vo.delete()

        ntype_vo: NetworkType = self.ntype_model.create(params)
        self.transaction.add_rollback(_rollback, ntype_vo)

        return ntype_vo

    def update_network_type(self, params):
        return self.update_network_type_by_vo(params,
                                              self.get_network_type(params.get('network_type_id'),
                                                                    params.get('domain_id')))

    def update_network_type_by_vo(self, params, network_type_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("name")} ({old_data.get("network_type_id")})')
            network_type_vo.update(old_data)

        self.transaction.add_rollback(_rollback, network_type_vo.to_dict())

        return network_type_vo.update(params)

    def delete_network_type(self, ntype_id, domain_id):
        self.delete_network_type_by_vo(self.get_network_type(ntype_id, domain_id))

    def get_network_type(self, network_type_id, domain_id, only=None):
        return self.ntype_model.get(network_type_id=network_type_id, domain_id=domain_id, only=only)

    def list_network_types(self, query):
        return self.ntype_model.query(**query)

    def stat_network_types(self, query):
        return self.ntype_model.stat(**query)

    @staticmethod
    def delete_network_type_by_vo(ntype_vo):
        ntype_vo.delete()
