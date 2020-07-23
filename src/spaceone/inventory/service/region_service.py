import logging
from spaceone.core.service import *
from spaceone.inventory.error import *
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.server_manager import ServerManager
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class RegionService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.region_mgr: RegionManager = self.locator.get_manager('RegionManager')

    @transaction
    @check_required(['name', 'region_code', 'region_type', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'region_code': 'str',
                    'region_type': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            region_vo (object)
        """

        region_mgr: RegionManager = self.locator.get_manager('RegionManager')
        return region_mgr.create_region(params)

    @transaction
    @check_required(['region_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'name': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            region_vo (object)

        """

        region_vo = self.region_mgr.get_region(params['region_id'], params['domain_id'])
        return self.region_mgr.update_region_by_vo(params, region_vo)

    @transaction
    @check_required(['region_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        region_vo = self.region_mgr.get_region(params['region_id'], params['domain_id'])

        is_exist, resource_id = self._check_resource_in_region(region_vo.region_code, region_vo.region_type,
                                                               params['domain_id'])

        if is_exist:
            raise ERROR_EXIST_RESOURCE(child=resource_id, parent=region_vo.region_id)

        self.region_mgr.delete_region_by_vo(region_vo)

    @transaction
    @check_required(['region_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            region_vo (object)

        """

        return self.region_mgr.get_region(params['region_id'], params['domain_id'], params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['region_id', 'name', 'region_code', 'region_type', 'domain_id'])
    @append_keyword_filter(['region_id', 'name'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'region_id': 'str',
                    'region_code': 'str',
                    'region_type': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.region_mgr.list_regions(params.get('query', {}))

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
        return self.region_mgr.stat_regions(query)

    def _check_resource_in_region(self, region_code, region_type, domain_id):
        server_mgr: ServerManager = self.locator.get_manager('ServerManager')
        cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')

        resource_query = {'filter': [{'k': 'state', 'v': 'DELETED', 'o': 'not'},
                                     {'k': 'region_code', 'v': region_code, 'o': 'eq'},
                                     {'k': 'region_type', 'v': region_type, 'o': 'eq'},
                                     {'k': 'domain_id', 'v': domain_id, 'o': 'eq'}]}

        server_vos, server_total_count = server_mgr.list_servers(query=resource_query)

        if server_total_count > 0:
            server_vo = server_vos[0]
            return True, server_vo.server_id

        cloud_svc_vos, cloud_svc_total_count = cloud_svc_mgr.list_cloud_services(query=resource_query)

        if cloud_svc_total_count > 0:
            cloud_svc_vo = cloud_svc_vos[0]
            return True, cloud_svc_vo.cloud_service_id

        return False, None
