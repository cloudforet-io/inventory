from spaceone.core.service import *
from spaceone.inventory.manager.ip_address_manager import IPManager
from spaceone.inventory.manager.subnet_manager import SubnetManager
from spaceone.inventory.manager.collection_data_manager import CollectionDataManager
from spaceone.inventory.lib.ip_address import IPAddress
from spaceone.inventory.error import *


@authentication_handler
@authorization_handler
@event_handler
class IPService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.ip_mgr: IPManager = self.locator.get_manager('IPManager')
        self.subnet_mgr: SubnetManager = self.locator.get_manager('SubnetManager')

    @transaction
    @check_required(['subnet_id', 'domain_id'])
    def allocate(self, params):
        """
        Args:
            params (dict): {
                    'ip_address': 'str',
                    'subnet_id': 'str',
                    'resource': 'dict',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            ip_address_vo (object)

        """

        collection_data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        job_id = self.transaction.get_meta('job_id')
        collector_id = self.transaction.get_meta('collector_id')
        secret_id = self.transaction.get_meta('secret.secret_id')
        service_account_id = self.transaction.get_meta('secret.service_account_id')

        domain_id = params['domain_id']

        params['collection_info'] = collection_data_mgr.create_new_history(params,
                                                                           domain_id,
                                                                           collector_id,
                                                                           service_account_id,
                                                                           secret_id,
                                                                           exclude_keys=['domain_id'])

        subnet_vo = self.subnet_mgr.get_subnet(params.get('subnet_id'), domain_id)

        if 'ip_address' in params:
            ip_vos, total_count = self._get_ip_by_subnet_vo(params.get('ip_address'), subnet_vo, domain_id)

            if total_count > 0:
                ip_vo = ip_vos[0]
                if ip_vo.state == 'ALLOCATED':
                    raise ERROR_ALREADY_USE_IP(ip=ip_vo.ip_address)
                elif ip_vo.state == 'RESERVED':
                    params['state'] = 'ALLOCATED'
                    return self.ip_mgr.update_ip_by_vo(params, ip_vo)
                else:
                    raise ERROR_INVALID_IP_STATE(ip=ip_vo.ip_address, state=ip_vo.state)
            else:
                self._check_valid_ip_in_subnet(params.get('ip_address'), subnet_vo)
                params['ip_int'] = int(IPAddress.get_ip_object(params.get('ip_address')))
        else:
            ip_addr, ip_address_int = self._generate_ip(subnet_vo)
            params.update({
                'ip_address': ip_addr,
                'ip_int': ip_address_int
            })

        params.update({
            'subnet': subnet_vo,
            'network': subnet_vo.network,
            'zone': subnet_vo.zone
        })

        return self.ip_mgr.allocate_ip(params)

    @transaction
    @check_required(['ip_address', 'subnet_id', 'domain_id'])
    def reserve(self, params):
        """
        Args:
            params (dict): {
                    'ip_address': 'str',
                    'subnet_id': 'str',
                    'tags': 'dict',
                    'user_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            ip_address_vo (object)

        """

        subnet_vo = self.subnet_mgr.get_subnet(params.get('subnet_id'), params.get('domain_id'))

        ip_vos, total_count = self._get_ip_by_subnet_vo(params.get('ip_address'), subnet_vo, params.get('domain_id'))

        if total_count > 0:
            raise ERROR_ALREADY_USE_IP(ip=params.get('ip_address'))

        self._check_valid_ip_in_subnet(params.get('ip_address'), subnet_vo)

        params.update({
            'ip_int': int(IPAddress.get_ip_object(params.get('ip_address'))),
            'user_id': self.transaction.get_meta('user_id'),
            'subnet': subnet_vo,
            'network': subnet_vo.network,
            'zone': subnet_vo.zone
        })

        return self.ip_mgr.reserve_ip(params)

    @transaction
    @check_required(['ip_address', 'subnet_id', 'domain_id'])
    def release(self, params):
        """
        Args:
            params (dict): {
                    'ip_address': 'str',
                    'subnet_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            ip_address_vo (object)

        """

        subnet_vo = self.subnet_mgr.get_subnet(params.get('subnet_id'), params.get('domain_id'))
        ip_vo = self.ip_mgr.get_ip_by_subnet_vo(params.get('ip_address'), params.get('domain_id'), subnet_vo)

        self.ip_mgr.release_ip_by_vo(ip_vo)

    @transaction
    @check_required(['ip_address', 'subnet_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'ip_address': 'str',
                    'subnet_id': 'str',
                    'resource': 'dict',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            ip_address_vo (object)

        """

        collection_data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        job_id = self.transaction.get_meta('job_id')
        collector_id = self.transaction.get_meta('collector_id')
        secret_id = self.transaction.get_meta('secret.secret_id')
        service_account_id = self.transaction.get_meta('secret.service_account_id')

        domain_id = params['domain_id']

        subnet_vo = self.subnet_mgr.get_subnet(params.get('subnet_id'), domain_id)
        ip_vo = self.ip_mgr.get_ip_by_subnet_vo(params.get('ip_address'), domain_id, subnet_vo)

        params = collection_data_mgr.exclude_data_by_pinned_keys(params, ip_vo.collection_info)
        params = collection_data_mgr.exclude_data_by_history(params,
                                                             ip_vo.to_dict(),
                                                             domain_id,
                                                             ip_vo.collection_info,
                                                             collector_id,
                                                             service_account_id,
                                                             secret_id,
                                                             exclude_keys=['ip_address', 'subnet_id', 'domain_id'])

        if ip_vo.state != 'ALLOCATED':
            raise ERROR_NOT_ALLOCATED_IP(ip=params['ip_address'])

        update_params = {}
        if 'resource' in params:
            update_params['resource'] = params['resource']

        if 'data' in params:
            update_params['data'] = collection_data_mgr.merge_data(ip_vo.data, params['data'])

        if 'metadata' in params:
            update_params['metadata'] = collection_data_mgr.merge_metadata(ip_vo.metadata, params['metadata'])

        return self.ip_mgr.update_ip_by_vo(update_params, ip_vo)

    @transaction
    @check_required(['ip_address', 'subnet_id', 'keys', 'domain_id'])
    def pin_data(self, params):
        """
        Args:
            params (dict): {
                    'subnet_id': 'str',
                    'ip_address': 'str',
                    'keys': 'list',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_vo (object)

        """

        domain_id = params['domain_id']

        collection_data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        subnet_vo = self.subnet_mgr.get_subnet(params.get('subnet_id'), domain_id)
        ip_vo = self.ip_mgr.get_ip_by_subnet_vo(params.get('ip_address'), domain_id, subnet_vo)

        params['collection_info'] = collection_data_mgr.update_pinned_keys(params['keys'],
                                                                           ip_vo.collection_info)

        return self.ip_mgr.update_ip_by_vo(params, ip_vo)

    @transaction
    @check_required(['ip_address', 'subnet_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'ip_address': 'str',
                    'subnet_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            ip_address_vo (object)

        """

        domain_id = params['domain_id']

        subnet_vo = self.subnet_mgr.get_subnet(params['subnet_id'], domain_id)

        return self.ip_mgr.get_ip_by_subnet_vo(params['ip_address'], domain_id, subnet_vo, params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['ip_address', 'state', 'subnet_id', 'network_id', 'zone_id',
                          'region_id', 'domain_id'])
    @append_keyword_filter(['ip_address', 'reference.resource_id'])

    def list(self, params):
        """
        Args:
            params (dict): {
                    'ip_address': 'str',
                    'state': 'ALLOCATED | RESERVED',
                    'subnet_id': 'str',
                    'zone_id': 'dict',
                    'region_id': 'dict',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count

        """

        return self.ip_mgr.list_ips(params.get('query', {}))

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
        return self.ip_mgr.stat_ips(query)

    @staticmethod
    def _check_ip(ip):
        if IPAddress.check_valid_ip(ip) is False:
            raise ERROR_INVALID_IP_ADDRESS(ip=ip)

    @staticmethod
    def _check_valid_ip_in_cidr(ip, cidr):
        if IPAddress.check_valid_ip_in_network(ip, cidr) is False:
            raise ERROR_INVALID_IP_IN_CIDR(ip=ip, cidr=cidr)

    def _get_ip_in_range(self, ip_range):
        pass

    def _check_valid_ip_in_subnet(self, ip, subnet_vo):
        self._check_ip(ip)
        self._check_valid_ip_in_cidr(ip, subnet_vo.cidr)

    def _get_used_ips_in_subnet(self, subnet_vo):
        return self.ip_mgr.list_ips({
            'filter':[{'k': 'subnet.id', 'v': subnet_vo.id, 'o': 'eq'}],
            'sort': {'key': 'ip_int'}
        })

    def _get_available_ip_subnet(self, subnet_vo, ip_range=None):
        used_ip_vos, total_count = self._get_used_ips_in_subnet(subnet_vo)
        used_ips_int = map(lambda used_ip_vo: int(IPAddress.get_ip_object(used_ip_vo.ip_int)), used_ip_vos)

        if ip_range:
            start_ip_int = int(IPAddress.get_ip_object(ip_range.start))
            end_ip_int = int(IPAddress.get_ip_object(ip_range.end) + 1)
        else:
            cidr_obj = IPAddress.get_network_object(subnet_vo.cidr)
            start_ip_int = int(cidr_obj[1])
            end_ip_int = int(cidr_obj[-1])

        available_ips_int = set(range(start_ip_int, end_ip_int)) - set(used_ips_int)
        if len(available_ips_int) == 0:
            return None, None
        else:
            available_ip_int = sorted(list(available_ips_int))[0]
            return str(IPAddress.get_ip_object(available_ip_int)), available_ip_int

    def _generate_ip(self, subnet_vo):
        ip_ranges = subnet_vo.ip_ranges

        if len(ip_ranges) == 0:
            available_ip, available_ip_int = self._get_available_ip_subnet(subnet_vo)
            if available_ip:
                return available_ip, available_ip_int
            else:
                raise ERROR_NOT_AVAILABLE_IP_IN_SUBNET(subnet_id=subnet_vo.subnet_id)

        for ip_range in ip_ranges:
            available_ip, available_ip_int = self._get_available_ip_subnet(subnet_vo, ip_range)
            if available_ip:
                return available_ip, available_ip_int

        raise ERROR_NOT_AVAILABLE_IP_IN_SUBNET(subnet_id=subnet_vo.subnet_id)

    def _get_ip_by_subnet_vo(self, ip, subnet_vo, domain_id):
        query = {
            'filter': [{'k': 'ip_address', 'v': ip, 'o': 'eq'},
                       {'k': 'subnet', 'v': subnet_vo.id, 'o': 'eq'},
                       {'k': 'domain_id', 'v': domain_id, 'o': 'eq'}]
        }
        return self.ip_mgr.list_ips(query)