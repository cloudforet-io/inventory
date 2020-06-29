from spaceone.core.service import *
from spaceone.inventory.manager.network_policy_manager import NetworkPolicyManager
from spaceone.inventory.manager.subnet_manager import SubnetManager
from spaceone.inventory.manager.zone_manager import ZoneManager
from spaceone.inventory.manager.collection_data_manager import CollectionDataManager
from spaceone.inventory.lib.ip_address import IPAddress
from spaceone.inventory.error import *


@authentication_handler
@authorization_handler
@event_handler
class NetworkPolicyService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.npolicy_mgr: NetworkPolicyManager = self.locator.get_manager('NetworkPolicyManager')

    @transaction
    @check_required(['name', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'routing_tables': 'list ([{cidr, destination, interface}, ...)',
                    'dns': 'list',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            network_policy_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        domain_id = params['domain_id']

        params['collection_info'] = data_mgr.create_new_history(params, exclude_keys=['domain_id'])

        zone_mgr: ZoneManager = self.locator.get_manager('ZoneManager')
        zone_vo = zone_mgr.get_zone(params.get('zone_id'), domain_id)

        params.update({
            'zone': zone_vo,
            'region': zone_vo.region,
            'dns': list(map(lambda dns: self._check_dns(dns), params.get('dns', [])))
        })

        list(map(lambda route: self._check_route_rule(route), params.get('routing_tables', [])))
        return self.npolicy_mgr.create_network_policy(params)

    @transaction
    @check_required(['network_policy_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'network_policy_id': 'str',
                    'name': 'str',
                    'routing_tables': 'list ([{cidr, destination, interface}, ...)',
                    'dns': 'list',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            network_policy_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        domain_id = params['domain_id']

        network_policy_vo = self.npolicy_mgr.get_network_policy(params.get('network_policy_id'), domain_id)

        exclude_keys = ['network_policy_id', 'domain_id']
        params = data_mgr.merge_data_by_history(params, network_policy_vo.to_dict(), exclude_keys=exclude_keys)

        list(map(lambda route: self._check_route_rule(route), params.get('routing_tables', [])))
        list(map(lambda dns: self._check_dns(dns), params.get('dns', [])))

        return self.npolicy_mgr.update_network_policy_by_vo(params, network_policy_vo)

    @transaction
    @check_required(['network_policy_id', 'keys', 'domain_id'])
    def pin_data(self, params):
        """
        Args:
            params (dict): {
                    'network_policy_id': 'str',
                    'keys': 'list',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        network_policy_vo = self.npolicy_mgr.get_network_policy(params.get('network_policy_id'), params.get('domain_id'))

        params['collection_info'] = data_mgr.update_pinned_keys(params['keys'],
                                                                network_policy_vo.collection_info.to_dict())

        return self.npolicy_mgr.update_network_policy_by_vo(params, network_policy_vo)

    @transaction
    @check_required(['network_policy_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'network_policy_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        npolicy_vo = self.npolicy_mgr.get_network_policy(params.get('network_policy_id'), params.get('domain_id'))
        self._check_subnet(npolicy_vo)
        self.npolicy_mgr.delete_network_policy_by_vo(npolicy_vo)

    @transaction
    @check_required(['network_policy_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'network_policy_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            network_policy_vo (object)

        """

        return self.npolicy_mgr.get_network_policy(params['network_policy_id'], params['domain_id'],
                                                   params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['network_policy_id', 'name', 'zone_id', 'region_id', 'domain_id'])
    @append_keyword_filter(['network_policy_id', 'name', 'reference.resource_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'network_policy_id': 'str',
                    'name': 'str',
                    'zone_id': 'str',
                    'region_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.npolicy_mgr.list_network_policies(params.get('query', {}))

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
        return self.npolicy_mgr.stat_network_policies(query)

    @staticmethod
    def _check_route_rule(route):
        cidr = IPAddress.check_valid_network(route.get('cidr'))
        destination = IPAddress.check_valid_ip(route.get('destination'))

        if cidr is False:
            raise ERROR_INVALID_NETWORK(cidr=route.get('cidr'))

        if destination is False:
            raise ERROR_INVALID_IP_ADDRESS(ip=route.get('destination'))

        route.update({
            'cidr': cidr,
            'destination': destination
        })

    @staticmethod
    def _check_dns(dns):
        dns = IPAddress.check_valid_ip(dns)

        if dns is False:
            raise ERROR_INVALID_IP_ADDRESS(ip=dns)

        return dns

    def _check_subnet(self, npolicy_vo):
        subnet_mgr: SubnetManager = self.locator.get_manager('SubnetManager')
        subnet_vos, total_count = subnet_mgr.list_subnets({'filter': [{'k': 'network_policy.network_policy_id',
                                                                       'v': npolicy_vo.network_policy_id,
                                                                       'o': 'eq'}]})

        if total_count > 0:
            raise ERROR_EXIST_SUBNET_IN_NETWORK_POLICY(subnet_id=subnet_vos[0].subnet_id,
                                                       network_policy_id=npolicy_vo.network_policy_id)
