from spaceone.core.service import *
from spaceone.inventory.manager.cloud_service_tag_manager import CloudServiceTagManager


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceTagService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_tag_mgr: CloudServiceTagManager = self.locator.get_manager('CloudServiceTagManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['cloud_service_id', 'key', 'provider', 'project_id', 'domain_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'key': 'str',
                    'provider': 'str',
                    'project_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                    'domain_id  ': 'str'
                }

        Returns:
            results (list)
            total_count (int)
        """
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
            }

        Returns:
            values (list) : 'list of statistics data'
        """
        query = params.get('query', {})
        return self.cloud_svc_tag_mgr.stat_cloud_svc_tags(query)
