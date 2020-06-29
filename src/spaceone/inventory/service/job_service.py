from spaceone.core.service import *
from spaceone.inventory.manager.collector_manager.job_manager import JobManager


@authentication_handler
@authorization_handler
@event_handler
class JobService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.job_mgr: JobManager = self.locator.get_manager('JobManager')

    @transaction
    @check_required(['domain_id'])
    @change_only_key({'collector_info': 'collector'}, key_path='query.only')
    @append_query_filter(['job_id', 'state', 'collect_mode', 'collector_id', 'resource_type', 'resource_id',
                          'domain_id'])
    @append_keyword_filter(['job_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'job_id': 'str',
                    'state': 'str',
                    'collect_mode': 'str',
                    'collector_id': 'dict',
                    'resource_type': 'str',
                    'resource_id': 'str',
                    'domain_id  ': 'str',
                    'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.job_mgr.list_jobs(params.get('query', {}))

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
        return self.job_mgr.stat_jobs(query)
