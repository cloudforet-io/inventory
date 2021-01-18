from spaceone.core.service import *
from spaceone.inventory.manager.collector_manager.job_task_manager import JobTaskManager


@authentication_handler
@authorization_handler
@event_handler
class JobTaskService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.job_task_mgr: JobTaskManager = self.locator.get_manager('JobTaskManager')

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['job_task_id', 'status', 'job_id', 'secret_id', 'provider',
                          'service_account_id', 'project_id', 'domain_id'])
    @append_keyword_filter(['job_task_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'job_task_id': 'str',
                    'status': 'str',
                    'job_id': 'str',
                    'secret_id': 'str',
                    'provider': 'str',
                    'service_account_id': 'str',
                    'project_id': 'str',
                    'domain_id  ': 'str',
                    'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.job_task_mgr.list(params.get('query', {}))

    @transaction
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['job_task_id'])
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
        return self.job_task_mgr.stat(query)
