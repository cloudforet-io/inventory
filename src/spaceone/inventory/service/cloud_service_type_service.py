from spaceone.core.service import *
from spaceone.core import utils
from spaceone.inventory.manager.cloud_service_type_manager import CloudServiceTypeManager
from spaceone.inventory.error import *

_KEYWORD_FILTER = ['cloud_service_type_id', 'name', 'group', 'service_code']


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceTypeService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.cloud_svc_type_mgr: CloudServiceTypeManager = self.locator.get_manager('CloudServiceTypeManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'provider', 'group', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'group': 'str',
                    'provider': 'str',
                    'service_code': 'str',
                    'is_primary': 'bool',
                    'is_major': 'bool',
                    'resource_type': 'str',
                    'metadata': 'dict',
                    'labels': 'list,
                    'tags': 'list or dict',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_type_vo (object)

        """

        params['updated_by'] = self.transaction.get_meta('collector_id') or 'manual'

        provider = params.get('provider', self.transaction.get_meta('secret.provider'))

        if provider:
            params['provider'] = provider

        if 'tags' in params:
            if isinstance(params['tags'], list):
                params['tags'] = utils.tags_to_dict(params['tags'])

        params['resource_type'] = params.get('resource_type', 'inventory.CloudService')

        params['ref_cloud_service_type'] = f'{params["domain_id"]}.{params["provider"]}.' \
                                           f'{params["group"]}.{params["name"]}'

        params['cloud_service_type_key'] = f'{params["provider"]}.{params["group"]}.{params["name"]}'

        return self.cloud_svc_type_mgr.create_cloud_service_type(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['cloud_service_type_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_type_id': 'str',
                    'service_code': 'str',
                    'is_primary': 'bool',
                    'is_major': 'bool',
                    'resource_type': 'str',
                    'metadata': 'dict',
                    'labels': 'list',
                    'tags': 'list or dict',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_type_vo (object)

        """

        if 'tags' in params:
            if isinstance(params['tags'], list):
                params['tags'] = utils.tags_to_dict(params['tags'])

        params['updated_by'] = self.transaction.get_meta('collector_id') or 'manual'

        provider = params.get('provider', self.transaction.get_meta('secret.provider'))
        domain_id = params['domain_id']

        cloud_svc_type_vo = self.cloud_svc_type_mgr.get_cloud_service_type(params['cloud_service_type_id'],
                                                                           domain_id)

        if not cloud_svc_type_vo.cloud_service_type_key:
            params['cloud_service_type_key'] = f'{cloud_svc_type_vo.provider}.{cloud_svc_type_vo.group}.' \
                                               f'{cloud_svc_type_vo.name}'

        return self.cloud_svc_type_mgr.update_cloud_service_type_by_vo(params, cloud_svc_type_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['cloud_service_type_id', 'domain_id'])
    def delete(self, params):

        """
        Args:
            params (dict): {
                    'cloud_service_type_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        cloud_svc_type_vo = self.cloud_svc_type_mgr.get_cloud_service_type(params['cloud_service_type_id'],
                                                                           params['domain_id'])

        self.cloud_svc_type_mgr.delete_cloud_service_type_by_vo(cloud_svc_type_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['cloud_service_type_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_type_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            cloud_service_type_vo (object)

        """

        return self.cloud_svc_type_mgr.get_cloud_service_type(params['cloud_service_type_id'], params['domain_id'],
                                                              params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['cloud_service_type_id', 'name', 'provider', 'group', 'cloud_service_type_key',
                          'service_code', 'is_primary', 'is_major', 'resource_type', 'domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    def list(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_type_id': 'str',
                    'name': 'str',
                    'group': 'str',
                    'provider': 'str',
                    'cloud_service_type_key': 'str',
                    'service_code': 'str',
                    'is_primary': 'str',
                    'is_major': 'str',
                    'resource_type': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.cloud_svc_type_mgr.list_cloud_service_types(params.get('query', {}))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
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
        return self.cloud_svc_type_mgr.stat_cloud_service_types(query)
