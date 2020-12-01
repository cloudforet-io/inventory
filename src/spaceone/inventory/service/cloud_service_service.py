from spaceone.core.service import *
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.manager.resource_group_manager import ResourceGroupManager
from spaceone.inventory.manager.collection_data_manager import CollectionDataManager
from spaceone.inventory.error import *

_KEYWORD_FILTER = ['cloud_service_id', 'provider', 'cloud_service_group', 'cloud_service_type',
                   'reference.resource_id']


@authentication_handler
@authorization_handler
@event_handler
class CloudServiceService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')
        self.region_mgr: RegionManager = self.locator.get_manager('RegionManager')
        self.identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')

    @transaction
    @check_required(['cloud_service_type', 'cloud_service_group', 'provider', 'data', 'domain_id'])
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
                    'tags': 'list',
                    'region_code': 'str',
                    'project_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_vo (object)

        """

        data_mgr: CollectionDataManager = self.locator.get_manager('CollectionDataManager')

        domain_id = params['domain_id']
        provider = params.get('provider', self.transaction.get_meta('secret.provider'))
        project_id = params.get('project_id')
        secret_project_id = self.transaction.get_meta('secret.project_id')
        region_code = params.get('region_code')

        # Temporary Code for Tag Migration
        tags = params.get('tags')

        if isinstance(tags, dict):
            change_tags = []
            for key, value in tags.items():
                change_tags.append({
                    'key': key,
                    'value': value
                })
            params['tags'] = change_tags

        if provider:
            params['provider'] = provider

        if project_id:
            self.identity_mgr.get_project(project_id, domain_id)
        elif secret_project_id:
            params['project_id'] = secret_project_id

        if region_code:
            params['ref_region'] = f'{domain_id}.{provider or "datacenter"}.{region_code}'

        params['ref_cloud_service_type'] = f'{params["domain_id"]}.' \
                                           f'{params["provider"]}.' \
                                           f'{params["cloud_service_group"]}.' \
                                           f'{params["cloud_service_type"]}'

        params = data_mgr.create_new_history(params,
                                             exclude_keys=['domain_id', 'ref_region', 'ref_cloud_service_type'])

        return self.cloud_svc_mgr.create_cloud_service(params)

    @transaction
    @check_required(['cloud_service_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'cloud_service_group': 'str',
                    'cloud_service_type': 'str',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'list',
                    'region_code': 'str',
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
        project_id = params.get('project_id')
        secret_project_id = self.transaction.get_meta('secret.project_id')

        domain_id = params['domain_id']
        release_region = params.get('release_region', False)
        release_project = params.get('release_project', False)
        region_code = params.get('region_code')
        cloud_service_group = params.get('cloud_service_group')
        cloud_service_type = params.get('cloud_service_type')

        cloud_svc_vo = self.cloud_svc_mgr.get_cloud_service(params['cloud_service_id'], domain_id)

        # Temporary Code for Tag Migration
        tags = params.get('tags')

        if isinstance(tags, dict):
            change_tags = []
            for key, value in tags.items():
                change_tags.append({
                    'key': key,
                    'value': value
                })
            params['tags'] = change_tags

        if provider:
            params['provider'] = provider

        if release_region:
            params.update({
                'region_code': None,
                'ref_region': None
            })
        else:
            if region_code:
                params['ref_region'] = f'{domain_id}.{provider or cloud_svc_vo.provider or "datacenter"}.{region_code}'

        if release_project:
            params['project_id'] = None
        elif project_id:
            self.identity_mgr.get_project(project_id, domain_id)
        elif secret_project_id:
            params['project_id'] = secret_project_id

        # if not cloud_svc_vo.ref_cloud_service_type:
        #     params['ref_cloud_service_type'] = f'{cloud_svc_vo.domain_id}.' \
        #                                        f'{cloud_svc_vo.provider}.' \
        #                                        f'{cloud_svc_vo.cloud_service_group}.' \
        #                                        f'{cloud_svc_vo.cloud_service_type}'

        if cloud_service_group and cloud_service_type:
            if provider or cloud_svc_vo.provider:
                params['ref_cloud_service_type'] = f'{params["domain_id"]}.' \
                                                   f'{provider or cloud_svc_vo.provider}.' \
                                                   f'{params["cloud_service_group"]}.' \
                                                   f'{params["cloud_service_type"]}'
            else:
                del params['cloud_service_group']
                del params['cloud_service_type']
        elif cloud_service_group and not cloud_service_type:
            del params['cloud_service_group']
        elif not cloud_service_group and cloud_service_type:
            del params['cloud_service_type']

        cloud_svc_data = cloud_svc_vo.to_dict()
        exclude_keys = ['cloud_service_id', 'domain_id', 'release_project', 'release_region',
                        'ref_region', 'ref_cloud_service_type']
        params = data_mgr.merge_data_by_history(params, cloud_svc_data, exclude_keys=exclude_keys)

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
    @append_query_filter(['cloud_service_id', 'state', 'cloud_service_type', 'cloud_service_group', 'provider',
                          'region_code', 'resource_group_id', 'project_id', 'domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(_KEYWORD_FILTER)
    def list(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'state': 'str',
                    'provider': 'str',
                    'cloud_service_type': 'str',
                    'cloud_service_group': 'str',
                    'region_code': 'str',
                    'resource_group_id': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        query = params.get('query', {})
        query = self._append_resource_group_filter(query, params['domain_id'])

        return self.cloud_svc_mgr.list_cloud_services(query)

    @transaction
    @check_required(['query', 'domain_id'])
    @append_query_filter(['resource_group_id', 'domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params):
        """
        Args:
            params (dict): {
                'resource_group_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        query = self._append_resource_group_filter(query, params['domain_id'])

        return self.cloud_svc_mgr.stat_cloud_services(query)

    def _append_resource_group_filter(self, query, domain_id):
        change_filter = []

        for condition in query.get('filter', []):
            key = condition.get('k', condition.get('key'))
            value = condition.get('v', condition.get('value'))
            operator = condition.get('o', condition.get('operator'))

            if key == 'resource_group_id':
                cloud_service_ids = None

                if operator in ['not', 'not_contain', 'not_in', 'not_contain_in']:
                    resource_group_operator = 'not_in'
                else:
                    resource_group_operator = 'in'

                if operator in ['eq', 'not', 'contain', 'not_contain']:
                    cloud_service_ids = self._get_cloud_service_ids_from_resource_group_id(value, domain_id)
                elif operator in ['in', 'not_in', 'contain_in', 'not_contain_in'] and isinstance(value, list):
                    cloud_service_ids = []
                    for v in value:
                        cloud_service_ids += self._get_cloud_service_ids_from_resource_group_id(v, domain_id)

                if cloud_service_ids is not None:
                    change_filter.append({
                        'k': 'cloud_service_id',
                        'v': list(set(cloud_service_ids)),
                        'o': resource_group_operator
                    })

            else:
                change_filter.append(condition)

        query['filter'] = change_filter
        return query

    def _get_cloud_service_ids_from_resource_group_id(self, resource_group_id, domain_id):
        resource_type = 'inventory.CloudService'
        rg_mgr: ResourceGroupManager = self.locator.get_manager('ResourceGroupManager')

        resource_group_filters = rg_mgr.get_resource_group_filter(resource_group_id, resource_type, domain_id,
                                                                  _KEYWORD_FILTER)
        cloud_service_ids = []
        for resource_group_query in resource_group_filters:
            resource_group_query['distinct'] = 'cloud_service_id'
            result = self.cloud_svc_mgr.stat_cloud_services(resource_group_query)
            cloud_service_ids += result.get('results', [])
        return cloud_service_ids
