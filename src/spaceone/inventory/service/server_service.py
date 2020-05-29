import logging

from spaceone.core.service import *
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.zone_manager import ZoneManager
from spaceone.inventory.manager.pool_manager import PoolManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.manager.collection_data_manager import CollectionDataManager
from spaceone.inventory.manager.server_manager import ServerManager
from spaceone.inventory.model.server_model import Server
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class ServerService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.server_mgr: ServerManager = self.locator.get_manager('ServerManager')

    @transaction
    @check_required(['domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'state': 'PENDING | INSERVICE',
                    'primary_ip_address': 'str',
                    'server_type': 'BAREMETAL | VM | HYPERVISOR | UNKNOWN',
                    'os_type': 'LINUX | WINDOWS',
                    'provider': 'str',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'nics': 'list',
                    'disks': 'list',
                    'tags': 'dict',
                    'asset_id': 'str',
                    'pool_id': 'str',
                    'zone_id': 'str',
                    'region_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            server_vo (object)

        """

        collection_data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        job_id = self.transaction.get_meta('job_id')
        collector_id = self.transaction.get_meta('collector_id')
        secret_id = self.transaction.get_meta('secret.secret_id')
        service_account_id = self.transaction.get_meta('secret.service_account_id')
        provider = params.get('provider', self.transaction.get_meta('secret.provider'))
        project_id = params.get('project_id', self.transaction.get_meta('secret.project_id'))

        domain_id = params['domain_id']
        region_id = params.get('region_id')
        zone_id = params.get('zone_id')
        pool_id = params.get('pool_id')
        nics = params.get('nics', [])
        primary_ip_address = params.get('primary_ip_address')

        params['state'] = params.get('state', 'INSERVICE')
        params['collection_info'] = collection_data_mgr.create_new_history(params,
                                                                           domain_id,
                                                                           collector_id,
                                                                           service_account_id,
                                                                           secret_id,
                                                                           exclude_keys=['domain_id'])

        if provider:
            params['provider'] = provider

        if pool_id:
            params.update(self._get_pool(pool_id, domain_id))
        else:
            if zone_id:
                params.update(self._get_zone(zone_id, domain_id))
            else:
                if region_id:
                    params.update(self._get_region(region_id, domain_id))

        if project_id:
            self._check_project(project_id, domain_id)
            params['project_id'] = project_id

        params['ip_addresses'] = self._get_ip_addresses_from_nics(nics)
        params['primary_ip_address'] = self._get_primary_ip_address(
            primary_ip_address, params['ip_addresses'])

        return self.server_mgr.create_server(params)

    @transaction
    @check_required(['server_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'server_id': 'str',
                    'name': 'str',
                    'state': 'INSERVICE | MAINTENANCE | CLOSED',
                    'primary_ip_address': 'str',
                    'server_type': 'BAREMETAL | VM | HYPERVISOR | UNKNOWN',
                    'os_type': 'LINUX | WINDOWS',
                    'provider': 'str',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'nics': 'list',
                    'disks': 'list',
                    'tags': 'dict',
                    'asset_id': 'str',
                    'pool_id': 'str',
                    'zone_id': 'str',
                    'region_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'release_project': 'bool',
                    'release_region': 'bool'
                }

        Returns:
            server_vo (object)

        """

        collection_data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        job_id = self.transaction.get_meta('job_id')
        collector_id = self.transaction.get_meta('collector_id')
        secret_id = self.transaction.get_meta('secret.secret_id')
        service_account_id = self.transaction.get_meta('secret.service_account_id')
        provider = params.get('provider', self.transaction.get_meta('secret.provider'))
        project_id = params.get('project_id', self.transaction.get_meta('secret.project_id'))

        domain_id = params['domain_id']
        region_id = params.get('region_id')
        zone_id = params.get('zone_id')
        pool_id = params.get('pool_id')
        release_region = params.get('release_region', False)
        release_project = params.get('release_project', False)
        primary_ip_address = params.get('primary_ip_address', None)

        server_vo: Server = self.server_mgr.get_server(params['server_id'], params['domain_id'])

        params = collection_data_mgr.exclude_data_by_pinned_keys(params, server_vo.collection_info)
        params = collection_data_mgr.exclude_data_by_history(params,
                                                             server_vo.to_dict(),
                                                             domain_id,
                                                             server_vo.collection_info,
                                                             collector_id,
                                                             service_account_id,
                                                             secret_id,
                                                             exclude_keys=['server_id', 'domain_id',
                                                                           'release_project', 'release_pool'])

        if 'data' in params:
            params['data'] = collection_data_mgr.merge_data(server_vo.data, params['data'])

        if 'metadata' in params:
            params['metadata'] = collection_data_mgr.merge_metadata(server_vo.metadata, params['metadata'])

        if provider:
            params['provider'] = provider

        if release_region:
            params.update({
                'pool': None,
                'zone': None,
                'region': None
            })

        else:
            if pool_id:
                params.update(self._get_pool(pool_id, domain_id))
            else:
                if zone_id:
                    params.update(self._get_zone(zone_id, domain_id))
                else:
                    if region_id:
                        params.update(self._get_region(region_id, domain_id))

        if release_project:
            params['project_id'] = None
        elif project_id:
            self._check_project(project_id, domain_id)
            params['project_id'] = project_id

        if 'nics' in params:
            params['ip_addresses'] = self._get_ip_addresses_from_nics(params['nics'])
            params['primary_ip_address'] = self._get_primary_ip_address(
                primary_ip_address, params['ip_addresses'], server_vo.primary_ip_address)

        else:
            if primary_ip_address:
                params['primary_ip_address'] = self._get_primary_ip_address(
                    primary_ip_address, server_vo.ip_addresses)

        return self.server_mgr.update_server_by_vo(params, server_vo)

    @transaction
    @check_required(['server_id', 'domain_id'])
    def pin_data(self, params):
        """
        Args:
            params (dict): {
                    'server_id': 'str',
                    'keys': 'list',
                    'domain_id': 'str'
                }

        Returns:
            server_vo (object)

        """

        collection_data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        server_vo: Server = self.server_mgr.get_server(params['server_id'], params['domain_id'])

        params['collection_info'] = collection_data_mgr.update_pinned_keys(params['keys'],
                                                                           server_vo.collection_info)

        return self.server_mgr.update_server_by_vo(params, server_vo)

    @transaction
    @check_required(['server_id', 'domain_id'])
    def delete(self, params: dict):
        """
        Args:
            params (dict): {
                    'server_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        self.server_mgr.delete_server(params['server_id'], params['domain_id'])

    @transaction
    @check_required(['server_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'server_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            server_vo (object)

        """

        return self.server_mgr.get_server(params['server_id'], params['domain_id'], params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['server_id', 'name', 'state', 'primary_ip_address',
                          'ip_addresses', 'server_type', 'os_type', 'provider',
                          'asset_id', 'region_id', 'zone_id', 'pool_id', 'project_id',
                          'resource_group_id', 'domain_id'])
    @append_keyword_filter(['server_id', 'name', 'ip_addresses', 'provider', 'reference.resource_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'server_id': 'str',
                    'name': 'str',
                    'state': 'INSERVICE | MAINTENANCE | CLOSED',
                    'primary_ip_address': 'str',
                    'ip_addresses': 'str',
                    'server_type': 'BAREMETAL | VM | HYPERVISOR | UNKNOWN',
                    'os_type': 'LINUX | WINDOWS',
                    'provider': 'str',
                    'asset_id': 'str',
                    'region_id': 'str',
                    'zone_id': 'str',
                    'pool_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'resource_group_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        query = params.get('query', {})
        return self.server_mgr.list_servers(query)

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
        return self.server_mgr.stat_servers(query)

    def _get_region(self, region_id, domain_id):
        region_mgr: RegionManager = self.locator.get_manager('RegionManager')
        region_vo = region_mgr.get_region(region_id, domain_id)

        return {
            'region': region_vo
        }

    def _get_zone(self, zone_id, domain_id):
        zone_mgr: ZoneManager = self.locator.get_manager('ZoneManager')
        zone_vo = zone_mgr.get_zone(zone_id, domain_id)

        return {
            'zone': zone_vo,
            'region': zone_vo.region
        }

    def _get_pool(self, pool_id, domain_id):
        pool_mgr: PoolManager = self.locator.get_manager('PoolManager')
        pool_vo = pool_mgr.get_pool(pool_id, domain_id)

        return {
            'pool': pool_vo,
            'zone': pool_vo.zone,
            'region': pool_vo.region
        }

    def _check_project(self, project_id, domain_id):
        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        identity_mgr.get_project(project_id, domain_id)

        return True

    @staticmethod
    def _get_ip_addresses_from_nics(nics: list) -> list:
        all_ip_addresses = []
        for nic in nics:
            for ip_info in nic.get('ip_addresses', []):
                ip_address = ip_info.get('ip_address')
                if ip_address:
                    all_ip_addresses.append(ip_address)

            public_ip_address = nic.get('public_ip_address')
            if public_ip_address:
                all_ip_addresses.append(public_ip_address)

        return all_ip_addresses

    @staticmethod
    def _get_primary_ip_address(primary_ip_address, all_ip_addresses, old_primary_ip_address=None):
        if primary_ip_address:
            if len(all_ip_addresses) > 0:
                if primary_ip_address not in all_ip_addresses:
                    raise ERROR_INVALID_PRIMARY_IP_ADDRESS()

            return primary_ip_address

        else:
            if len(all_ip_addresses) > 0:
                if old_primary_ip_address and old_primary_ip_address in all_ip_addresses:
                    return old_primary_ip_address
                else:
                    return all_ip_addresses[0]

        raise ERROR_REQUIRED_IP_ADDRESS()
