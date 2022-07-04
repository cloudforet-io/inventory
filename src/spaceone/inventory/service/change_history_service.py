from spaceone.core.service import *
from spaceone.inventory.manager.change_history_manager import ChangeHistoryManager


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ChangeHistoryService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.cm_mgr: ChangeHistoryManager = self.locator.get_manager('ChangeHistoryManager')

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'mutation.append_parameter': {'user_projects': 'authorization.projects'}
    })
    @check_required(['domain_id'])
    @change_only_key({'collector_info': 'collector'}, key_path='query.only')
    @append_query_filter(['record_id', 'cloud_service_id', 'action', 'user_id', 'collector_id', 'job_id',
                          'domain_id', 'user_projects'])
    @append_keyword_filter(['record_id', 'cloud_service_id'])
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
                    'domain_id  ': 'str',
                    'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                    'user_projects': 'list', // from meta
                }

        Returns:
            results (list)
            total_count (int)

        """
        query = params.get('query', {})

        return self.cm_mgr.list_records(query)

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'mutation.append_parameter': {'user_projects': 'authorization.projects'}
    })
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(['record_id', 'cloud_service_id'])
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

        query = params.get('query', {})
        return self.cm_mgr.stat_records(query)
