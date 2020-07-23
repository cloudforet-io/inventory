import logging

from spaceone.core.service import *
from spaceone.inventory.manager.region_manager import RegionManager
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
        self.region_mgr: RegionManager = self.locator.get_manager('RegionManager')
        self.identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')

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
                    'region_code': 'str',
                    'region_type': 'str',
                    'project_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            server_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        provider = params.get('provider', self.transaction.get_meta('secret.provider'))
        project_id = params.get('project_id', self.transaction.get_meta('secret.project_id'))

        domain_id = params['domain_id']
        nics = params.get('nics', [])
        primary_ip_address = params.get('primary_ip_address')

        params['state'] = params.get('state', 'INSERVICE')

        if provider:
            params['provider'] = provider

        if 'region_code' in params and 'region_type' not in params:
            raise ERROR_REQUIRED_PARAMETER(key='region_type')

        if 'region_type' in params and 'region_code' not in params:
            raise ERROR_REQUIRED_PARAMETER(key='region_code')

        if 'region_code' in params and 'region_type' in params:
            # Validation Check
            self.region_mgr.get_region_from_code(params['region_code'], params['region_type'], domain_id)
            params['region_ref'] = f'{params["region_type"]}.{params["region_code"]}'

        if project_id:
            self.identity_mgr.get_project(project_id, domain_id)
            params['project_id'] = project_id

        params['ip_addresses'] = self._get_ip_addresses_from_nics(nics)
        params['primary_ip_address'] = self._get_primary_ip_address(
            primary_ip_address, params['ip_addresses'])

        params['collection_info'] = data_mgr.create_new_history(params, exclude_keys=['domain_id'])

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
                    'project_id': 'str',
                    'domain_id': 'str',
                    'release_project': 'bool',
                    'release_region': 'bool'
                }

        Returns:
            server_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        provider = params.get('provider', self.transaction.get_meta('secret.provider'))
        project_id = params.get('project_id', self.transaction.get_meta('secret.project_id'))

        domain_id = params['domain_id']
        release_region = params.get('release_region', False)
        release_project = params.get('release_project', False)
        primary_ip_address = params.get('primary_ip_address', None)

        server_vo: Server = self.server_mgr.get_server(params['server_id'], params['domain_id'])

        if provider:
            params['provider'] = provider

        if release_region:
            params.update({
                'region_code': None,
                'region_type': None,
                'region_ref': None
            })

        if release_project:
            params['project_id'] = None
        elif project_id:
            self.identity_mgr.get_project(project_id, domain_id)
            params['project_id'] = project_id

        if 'nics' in params:
            params['ip_addresses'] = self._get_ip_addresses_from_nics(params['nics'])
            params['primary_ip_address'] = self._get_primary_ip_address(
                primary_ip_address, params['ip_addresses'], server_vo.primary_ip_address)

        else:
            if primary_ip_address:
                params['primary_ip_address'] = self._get_primary_ip_address(
                    primary_ip_address, server_vo.ip_addresses)

        server_data = server_vo.to_dict()
        exclude_keys = ['server_id', 'domain_id', 'release_project', 'release_pool']
        params = data_mgr.merge_data_by_history(params, server_data, exclude_keys=exclude_keys)

        _LOGGER.debug("------- PARAMS with Update history ------")
        _LOGGER.debug(params)
        _LOGGER.debug("-----------------------------------------")

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

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        server_vo: Server = self.server_mgr.get_server(params['server_id'], params['domain_id'])

        params['collection_info'] = data_mgr.update_pinned_keys(params['keys'], server_vo.collection_info.to_dict())

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
    @change_only_key({'region_info': 'region', 'zone_info': 'zone', 'pool_info': 'pool'})
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
    @change_only_key({'region_info': 'region', 'zone_info': 'zone', 'pool_info': 'pool'}, key_path='query.only')
    @append_query_filter(['server_id', 'name', 'state', 'primary_ip_address',
                          'ip_addresses', 'server_type', 'os_type', 'provider',
                          'asset_id', 'region_code', 'region_type', 'project_id',
                          'resource_group_id', 'domain_id'])
    @append_keyword_filter(['server_id', 'name', 'ip_addresses', 'provider', 'reference.resource_id',
                            'project_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'server_id': 'str',
                    'name': 'str',
                    'state': 'INSERVICE | MAINTENANCE | CLOSED | DELETED',
                    'primary_ip_address': 'str',
                    'ip_addresses': 'str',
                    'server_type': 'BAREMETAL | VM | HYPERVISOR | UNKNOWN',
                    'os_type': 'LINUX | WINDOWS',
                    'provider': 'str',
                    'asset_id': 'str',
                    'region_code': 'str',
                    'region_type': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'resource_group_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.server_mgr.list_servers(params.get('query', {}))

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

        return self.server_mgr.stat_servers(params.get('query', {}))

    @staticmethod
    def _get_ip_addresses_from_nics(nics: list) -> list:
        all_ip_addresses = []
        try:
            for nic in nics:
                ip_addresses = nic.get('ip_addresses', [])
                all_ip_addresses += ip_addresses

                public_ip_address = nic.get('public_ip_address')
                if public_ip_address:
                    all_ip_addresses.append(public_ip_address)

            return list(set(all_ip_addresses))

        except Exception:
            raise ERROR_INVALID_PARAMETER(key='nics', reason='nics format is invalid.')

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
