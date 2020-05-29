from spaceone.core.service import *
from spaceone.inventory.manager.network_type_manager import NetworkTypeManager
from spaceone.inventory.manager.subnet_manager import SubnetManager
from spaceone.inventory.error import *


@authentication_handler
@authorization_handler
@event_handler
class NetworkTypeService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.ntype_mgr: NetworkTypeManager = self.locator.get_manager('NetworkTypeManager')

    @transaction
    @check_required(['name', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            network_type_vo (object)

        """

        return self.ntype_mgr.create_network_type(params)

    @transaction
    @check_required(['network_type_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'network_type_id': 'str',
                    'name': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            network_type_vo (object)

        """

        return self.ntype_mgr.update_network_type_by_vo(params,
                                                        network_type_vo=self.ntype_mgr.get_network_type(
                                                            params.get('network_type_id'),
                                                            params.get('domain_id')))

    @transaction
    @check_required(['network_type_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'network_type_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        ntype_vo = self.ntype_mgr.get_network_type(params.get('network_type_id'), params.get('domain_id'))
        self._check_subnet(ntype_vo)
        self.ntype_mgr.delete_network_type_by_vo(ntype_vo)

    @transaction
    @check_required(['network_type_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'network_type_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            network_type_vo (object)

        """

        return self.ntype_mgr.get_network_type(params['network_type_id'], params['domain_id'], params.get('only'))

    @transaction
    @append_query_filter(['network_type_id', 'name', 'domain_id'])
    @append_keyword_filter(['network_type_id', 'name'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'network_type_id': 'str',
                    'name': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.ntype_mgr.list_network_types(params.get('query', {}))

    @transaction
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.ntype_mgr.stat_network_types(query)

    def _check_subnet(self, ntype_vo):
        subnet_mgr: SubnetManager = self.locator.get_manager('SubnetManager')
        subnet_vos, total_count = subnet_mgr.list_subnets({'filter': [{'k': 'network_type.network_type_id',
                                                                       'v': ntype_vo.network_type_id,
                                                                       'o': 'eq'}]})

        if total_count > 0:
            raise ERROR_EXIST_SUBNET_IN_NETWORK_TYPE(subnet_id=subnet_vos[0].subnet_id,
                                                     network_type_id=ntype_vo.network_type_id)
