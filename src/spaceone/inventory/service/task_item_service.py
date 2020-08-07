from spaceone.core.service import *
from spaceone.inventory.manager.collector_manager.task_item_manager import TaskItemManager


@authentication_handler
@authorization_handler
@event_handler
class TaskItemService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.task_item_mgr: TaskItemManager = self.locator.get_manager('TaskItemManager')

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['resource_id', 'resource_type', 'job_task_id', 'job_id', 'state', 'domain_id'])
    @append_keyword_filter(['job_task_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'resource_id': 'str',
                    'resource_type': 'str',
                    'job_task_id': 'str',
                    'job_id': 'str',
                    'state': 'str',
                    'domain_id  ': 'str',
                    'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.task_item_mgr.list(params.get('query', {}))

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
        return self.task_item_mgr.stat(query)
