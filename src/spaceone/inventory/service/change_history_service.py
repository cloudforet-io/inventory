from spaceone.core.service import *
from spaceone.inventory.manager.record_manager import RecordManager
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ChangeHistoryService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.record_mgr: RecordManager = self.locator.get_manager('RecordManager')
        self.cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['cloud_service_id', 'domain_id'])
    @change_only_key({'collector_info': 'collector'}, key_path='query.only')
    @append_query_filter(['record_id', 'cloud_service_id', 'action', 'user_id', 'collector_id', 'job_id',
                          'updated_by', 'domain_id'])
    @append_keyword_filter(['diff.key', 'diff.before', 'diff.after'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'record_id': 'str',
                    'cloud_service_id': 'str',
                    'action': 'str',
                    'user_id': 'dict',
                    'collector_id': 'str',
                    'job_id': 'str',
                    'updated_by': 'str',
                    'domain_id  ': 'str',
                    'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                    'user_projects': 'list', // from meta
                }

        Returns:
            results (list)
            total_count (int)

        """

        self._check_cloud_service(params)

        query = params.get('query', {})
        return self.record_mgr.list_records(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(['diff.key', 'diff.before', 'diff.after'])
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

        self._check_cloud_service(params)

        query = params.get('query', {})
        return self.record_mgr.stat_records(query)

    def _check_cloud_service(self, params):
        cloud_service_id = params['cloud_service_id']
        domain_id = params['domain_id']
        user_projects = params.get('user_projects')
        self.cloud_svc_mgr.get_cloud_service(cloud_service_id, domain_id, user_projects=user_projects)
