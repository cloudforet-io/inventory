from spaceone.core.service import *
from spaceone.inventory.model.job_task_model import JobTask
from spaceone.inventory.manager.collector_manager.job_task_manager import JobTaskManager


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class JobTaskService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.job_task_mgr: JobTaskManager = self.locator.get_manager('JobTaskManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['job_task_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'job_task_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """
        job_task_id = params['job_task_id']
        domain_id = params['domain_id']

        job_task_vo: JobTask = self.job_task_mgr.get(job_task_id, domain_id)
        self.job_task_mgr.delete_job_task_by_vo(job_task_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['job_task_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'job_task_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            job_task_vo (object)

        """

        return self.job_task_mgr.get(params['job_task_id'], params['domain_id'], params.get('only'))

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['job_task_id', 'status', 'job_id', 'secret_id', 'provider',
                          'service_account_id', 'project_id', 'domain_id', 'user_projects'])
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
                    'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                    'user_projects': 'list', // from meta
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.job_task_mgr.list(params.get('query', {}))

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(['job_task_id'])
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
        return self.job_task_mgr.stat(query)
