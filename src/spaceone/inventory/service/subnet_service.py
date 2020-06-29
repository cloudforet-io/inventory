from spaceone.core.service import *
from spaceone.inventory.manager.subnet_manager import SubnetManager
from spaceone.inventory.manager.network_manager import NetworkManager
from spaceone.inventory.manager.network_type_manager import NetworkTypeManager
from spaceone.inventory.manager.network_policy_manager import NetworkPolicyManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.manager.collection_data_manager import CollectionDataManager
from spaceone.inventory.lib.ip_address import IPAddress
from spaceone.inventory.error import *


@authentication_handler
@authorization_handler
@event_handler
class SubnetService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.subnet_mgr: SubnetManager = self.locator.get_manager('SubnetManager')

    @transaction
    @check_required(['cidr', 'network_id', 'network_type_id', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'cidr': 'cidr',
                    'ip_ranges': 'list ([{start, end}, ...])',
                    'gateway': 'str',
                    'vlan': 'int',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'dict',
                    'network_id': 'str',
                    'network_type_id': 'str',
                    'network_policy_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            subnet_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        project_id = params.get('project_id', self.transaction.get_meta('secret.project_id'))

        domain_id = params['domain_id']

        params['collection_info'] = data_mgr.create_new_history(params, exclude_keys=['domain_id'])

        network_mgr: NetworkManager = self.locator.get_manager('NetworkManager')
        ntype_mgr: NetworkTypeManager = self.locator.get_manager('NetworkTypeManager')
        npolicy_mgr: NetworkPolicyManager = self.locator.get_manager('NetworkPolicyManager')
        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')

        net_vo = network_mgr.get_network(params.get('network_id'), domain_id)
        subnet_cidrs = self._get_cidrs_in_network(net_vo)

        params['cidr'] = self._check_cidr(params.get('cidr'), net_vo.cidr, subnet_cidrs)
        list(map(lambda ip_range: self._check_ip_range(ip_range, params.get('cidr')), params.get('ip_ranges', [])))

        if 'gateway' in params:
            self._check_ip(params.get('gateway'))

        if 'vlan' in params:
            self._check_vlan(params.get('vlan'))

        params.update({
            'network_type': ntype_mgr.get_network_type(params.get('network_type_id'), domain_id),
            'network': net_vo,
            'zone': net_vo.zone,
            'region': net_vo.region
        })

        if 'network_policy_id' in params:
            params['network_policy'] = npolicy_mgr.get_network_policy(params.get('network_policy_id'),
                                                                      domain_id)

        if project_id:
            identity_mgr.get_project(project_id, domain_id)
            params['project_id'] = project_id

        return self.subnet_mgr.create_subnet(params)

    @transaction
    @check_required(['subnet_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'subnet_id': 'str',
                    'name': 'str',
                    'ip_ranges': 'list ([{start, end}, ...])',
                    'gateway': 'str',
                    'vlan': 'int',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'dict',
                    'network_type_id': 'str',
                    'network_policy_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'release_project': 'bool'
                }

        Returns:
            network_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        project_id = params.get('project_id', self.transaction.get_meta('secret.project_id'))

        domain_id = params['domain_id']
        release_project = params.get('release_project', False)

        subnet_vo = self.subnet_mgr.get_subnet(params.get('subnet_id'), domain_id)

        ntype_mgr: NetworkTypeManager = self.locator.get_manager('NetworkTypeManager')
        npolicy_mgr: NetworkPolicyManager = self.locator.get_manager('NetworkPolicyManager')
        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')

        list(map(lambda ip_range: self._check_ip_range(ip_range, subnet_vo.cidr), params.get('ip_ranges', [])))

        if 'gateway' in params:
            self._check_ip(params['gateway'])

        if 'vlan' in params:
            self._check_vlan(params['vlan'])

        if 'network_policy_id' in params:
            params['network_policy'] = npolicy_mgr.get_network_policy(params['network_policy_id'], domain_id)

        if 'network_type_id' in params:
            params['network_type'] = ntype_mgr.get_network_type(params['network_type_id'], domain_id)

        if release_project:
            params['project_id'] = None
        elif project_id:
            identity_mgr.get_project(project_id, domain_id)
            params['project_id'] = project_id

        exclude_keys = ['subnet_id', 'domain_id', 'release_project']
        params = data_mgr.merge_data_by_history(params, subnet_vo.to_dict(), exclude_keys=exclude_keys)

        return self.subnet_mgr.update_subnet_by_vo(params, subnet_vo)

    @transaction
    @check_required(['subnet_id', 'keys', 'domain_id'])
    def pin_data(self, params):
        """
        Args:
            params (dict): {
                    'subnet_id': 'str',
                    'keys': 'list',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        subnet_vo = self.subnet_mgr.get_subnet(params.get('subnet_id'), params['domain_id'])

        params['collection_info'] = data_mgr.update_pinned_keys(params['keys'],
                                                                subnet_vo.collection_info.to_dict())

        return self.subnet_mgr.update_subnet_by_vo(params, subnet_vo)

    @transaction
    @check_required(['subnet_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'subnet_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        subnet_vo = self.subnet_mgr.get_subnet(params.get('subnet_id'), params.get('domain_id'))
        self._check_allocated_ip(subnet_vo)

        self.subnet_mgr.delete_subnet_by_vo(subnet_vo)

    @transaction
    @check_required(['subnet_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'subnet_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            subnet_vo (object)

        """

        return self.subnet_mgr.get_subnet(params['subnet_id'], params['domain_id'], params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['subnet_id', 'name', 'cidr', 'gateway', 'vlan', 'network_id',
                          'zone_id', 'region_id', 'network_type_id', 'network_policy_id',
                          'project_id', 'domain_id'])
    @append_keyword_filter(['subnet_id', 'name', 'cidr', 'reference.resource_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'subnet_id': 'str',
                    'name': 'str',
                    'cidr': 'str',
                    'gateway': 'str',
                    'vlan': 'int',
                    'network_id': 'str',
                    'zone_id': 'str',
                    'region_id_id': 'str',
                    'network_type_id': 'str',
                    'network_policy_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'

                }

        Returns:
            results (list)
            total_count (int)
        """

        return self.subnet_mgr.list_subnets(params.get('query', {}))

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
        return self.subnet_mgr.stat_subnets(query)

    @staticmethod
    def _check_duplicated_cidr(cidr, subnet_cidr):
        if IPAddress.check_duplicate_cidr_range(cidr, subnet_cidr):
            raise ERROR_DUPLICATE_CIDR(cidr1=cidr, cidr2=subnet_cidr)

    def _check_cidr(self, cidr, net_cidr, subnet_cidrs):
        _cidr = IPAddress.check_valid_network(cidr)

        if _cidr is False:
            raise ERROR_INVALID_NETWORK(cidr=cidr)

        if IPAddress.check_subnet_of_network(_cidr, net_cidr) is False:
            raise ERROR_INVALID_CIDR_IN_NETWORK(cidr=_cidr)

        list(map(lambda subnet_cidr: self._check_duplicated_cidr(cidr, subnet_cidr), subnet_cidrs))

        return _cidr

    @staticmethod
    def _check_ip(ip):
        if IPAddress.check_valid_ip(ip) is False:
            raise ERROR_INVALID_IP_ADDRESS(ip=ip)

    @staticmethod
    def _check_ip_in_cidr(ip, cidr):
        if IPAddress.check_valid_ip_in_network(ip, cidr) is False:
            raise ERROR_INVALID_IP_IN_CIDR(ip=ip, cidr=cidr)

    @staticmethod
    def _check_start_end_ip(start, end):
        start_ip_obj = IPAddress.get_ip_object(start)
        end_ip_obj = IPAddress.get_ip_object(end)

        if start_ip_obj > end_ip_obj:
            raise ERROR_INVALID_IP_RANGE(start=start, end=end)

    def _check_allocated_ip(self, subnet_vo):
        ip_mgr = self.locator.get_manager('IPManager')
        ip_vos, total_count = ip_mgr.list_ips({'filter': [{'k': 'zone',
                                                           'v': subnet_vo.id,
                                                           'o': 'eq'}]})

        if total_count > 0:
            raise ERROR_EXIST_RESOURCE(key='ip_address', value=ip_vos[0].ip_address)

    def _check_ip_range(self, ip_range, cidr):
        self._check_ip(ip_range.get('start'))
        self._check_ip(ip_range.get('end'))

        self._check_ip_in_cidr(ip_range.get('start'), cidr)
        self._check_ip_in_cidr(ip_range.get('end'), cidr)
        self._check_start_end_ip(ip_range.get('start'), ip_range.get('end'))

    def _get_cidrs_in_network(self, net_vo):
        return list(map(lambda subnet_vo: subnet_vo.cidr,
                        self._get_existed_subnets_in_network(net_vo)))

    def _get_existed_subnets_in_network(self, net_vo):
        exist_subnet_vos, total_count = self.subnet_mgr.list_subnets(
            {'filter': [{'k': 'network',
                         'v': net_vo.id,
                         'o': 'eq'}]})

        return exist_subnet_vos

    @staticmethod
    def _check_vlan(vlan):
        if vlan < 0 or vlan > 4095:
            raise ERROR_INVALID_VLAN(vlan=vlan)
