from spaceone.core.service import *
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.manager.collection_data_manager import CollectionDataManager


@authentication_handler
@authorization_handler
@event_handler
class CloudServiceService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')

    @transaction
    @check_required(['data', 'provider', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_type': 'str',
                    'cloud_service_group': 'str',
                    'provider': 'str',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'dict',
                    'region_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        provider = params.get('provider', self.transaction.get_meta('secret.provider'))
        project_id = params.get('project_id', self.transaction.get_meta('secret.project_id'))

        domain_id = params['domain_id']

        params['collection_info'] = data_mgr.create_new_history(params, exclude_keys=['domain_id'])

        if provider:
            params['provider'] = provider

        if 'region_id' in params:
            region_mgr: RegionManager = self.locator.get_manager('RegionManager')
            params['region'] = region_mgr.get_region(params['region_id'], domain_id)
            del params['region_id']

        if project_id:
            identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
            identity_mgr.get_project(project_id, domain_id)
            params['project_id'] = project_id

        return self.cloud_svc_mgr.create_cloud_service(params)

    @transaction
    @check_required(['cloud_service_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'dict',
                    'region_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'release_project': 'bool',
                    'release_region': 'bool'
                }

        Returns:
            cloud_service_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        provider = params.get('provider', self.transaction.get_meta('secret.provider'))
        project_id = params.get('project_id', self.transaction.get_meta('secret.project_id'))

        domain_id = params['domain_id']
        region_id = params.get('region_id')
        release_region = params.get('release_region', False)
        release_project = params.get('release_project', False)

        cloud_svc_vo = self.cloud_svc_mgr.get_cloud_service(params['cloud_service_id'], domain_id)

        if provider:
            params['provider'] = provider

        if release_region:
            params['region'] = None
        else:
            if region_id:
                region_mgr: RegionManager = self.locator.get_manager('RegionManager')
                params['region'] = region_mgr.get_region(region_id, domain_id)
                del params['region_id']

        if release_project:
            params['project_id'] = None
        elif project_id:
            identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
            identity_mgr.get_project(project_id, domain_id)
            params['project_id'] = project_id

        exclude_keys = ['cloud_service_id', 'domain_id', 'release_project', 'release_pool']
        params = data_mgr.merge_data_by_history(params, cloud_svc_vo.to_dict(), exclude_keys=exclude_keys)

        return self.cloud_svc_mgr.update_cloud_service_by_vo(params, cloud_svc_vo)

    @transaction
    @check_required(['cloud_service_id', 'keys', 'domain_id'])
    def pin_data(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'keys': 'list',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        cloud_svc_vo = self.cloud_svc_mgr.get_cloud_service(params['cloud_service_id'], params['domain_id'])

        params['collection_info'] = data_mgr.update_pinned_keys(params['keys'], cloud_svc_vo.collection_info.to_dict())

        return self.cloud_svc_mgr.update_cloud_service_by_vo(params, cloud_svc_vo)

    @transaction
    @check_required(['cloud_service_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        cloud_svc_vo = self.cloud_svc_mgr.get_cloud_service(params['cloud_service_id'],
                                                            params['domain_id'])

        self.cloud_svc_mgr.delete_cloud_service_by_vo(cloud_svc_vo)

    @transaction
    @check_required(['cloud_service_id', 'domain_id'])
    @change_only_key({'region_info': 'region'})
    def get(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            cloud_service_vo (object)

        """

        return self.cloud_svc_mgr.get_cloud_service(params['cloud_service_id'], params['domain_id'],
                                                    params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @change_only_key({'region_info': 'region'}, key_path='query.only')
    @append_query_filter(['cloud_service_id', 'cloud_service_type', 'provider', 'cloud_service_group',
                          'region_id', 'project_id', 'domain_id'])
    @append_keyword_filter(['cloud_service_id', 'cloud_service_type', 'provider', 'cloud_service_group',
                            'reference.resource_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'cloud_service_type': 'str',
                    'cloud_service_group': 'str'Add a reference field to all inventory resources,
                    'provider': 'str',
                    'region_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        query = self._append_state_query(params.get('query', {}))    # Append Query for DELETED filter (Temporary Logic)
        return self.cloud_svc_mgr.list_cloud_services(query)

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
        return self.cloud_svc_mgr.stat_cloud_services(query)

    '''
    TEMPORARY Logic for DELETED filter  
    '''
    @staticmethod
    def _append_state_query(query):
        state_defaul_filter = {
            'key': 'state',
            'value': 'DELETED',
            'operator': 'not'
        }

        deleted_display = False
        for _q in query.get('filter', []):
            key = _q.get('k', _q.get('key'))
            value = _q.get('v', _q.get('value'))
            operator = _q.get('o', _q.get('operator'))

            if key == 'state' and value == 'DELETED' and operator == 'eq':
                deleted_display = True
            if key == 'state' and value == ['DELETED'] and operator == 'in':
                deleted_display = True

        if deleted_display is False:
            _filter = query.get('filter', None)
            if _filter is None:
                query['filter'] = [state_defaul_filter]
            else:
                _filter.append(state_defaul_filter)

        return query
