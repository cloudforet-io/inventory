from spaceone.core.service import *
from spaceone.inventory.model.job_model import Job
from spaceone.inventory.manager.collector_manager.job_manager import JobManager


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class JobService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.job_mgr: JobManager = self.locator.get_manager('JobManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['job_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'job_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """
        job_id = params['job_id']
        domain_id = params['domain_id']

        job_vo: Job = self.job_mgr.get(job_id, domain_id)
        self.job_mgr.delete_job_by_vo(job_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['job_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'job_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            job_vo (object)

        """

        return self.job_mgr.get(params['job_id'], params['domain_id'], params.get('only'))

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @change_only_key({'collector_info': 'collector'}, key_path='query.only')
    @append_query_filter(['job_id', 'status', 'collector_id', 'project_id', 'domain_id', 'user_projects'])
    @append_keyword_filter(['job_id'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'job_id': 'str',
                    'status': 'str',
                    'collector_id': 'dict',
                    'project_id': 'str',
                    'domain_id  ': 'str',
                    'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                    'user_projects': 'list', // from meta
                }

        Returns:
            results (list)
            total_count (int)

        """
        query = params.get('query', {})

        # Temporary code for DB migration
        if 'only' in query:
            query['only'] += ['collector_id']

        return self.job_mgr.list_jobs(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(['job_id'])
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
        return self.job_mgr.stat_jobs(query)
