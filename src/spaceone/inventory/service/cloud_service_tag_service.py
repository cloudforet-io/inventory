from spaceone.core.service import *
from spaceone.inventory.manager.cloud_service_tag_manager import CloudServiceTagManager
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceTagService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_tag_mgr: CloudServiceTagManager = self.locator.get_manager('CloudServiceTagManager')
        self.cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['cloud_service_id', 'key', 'provider', 'domain_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'key': 'str',
                    'provider': 'str',
                    'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                    'domain_id  ': 'str',
                    'user_projects': 'list', // from meta
                }

        Returns:
            results (list)
            total_count (int)
        """

        if 'cloud_service_id' in params:
            self._check_cloud_service(params)

        query = params.get('query', {})
        return self.cloud_svc_tag_mgr.list_cloud_svc_tags(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'
        """
        if 'cloud_service_id' in params:
            self._check_cloud_service(params)

        query = params.get('query', {})
        return self.cloud_svc_tag_mgr.stat_cloud_svc_tags(query)

    def _check_cloud_service(self, params):
        cloud_service_id = params['cloud_service_id']
        domain_id = params['domain_id']
        user_projects = params.get('user_projects')
        self.cloud_svc_mgr.get_cloud_service(cloud_service_id, domain_id, user_projects=user_projects)
