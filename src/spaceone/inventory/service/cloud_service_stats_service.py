from spaceone.core.service import *
from spaceone.inventory.error import *
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.manager.cloud_service_stats_manager import CloudServiceStatsManager


_KEYWORD_FILTER = ['query_set_id', 'name']


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceStatsService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_stats_mgr: CloudServiceStatsManager = self.locator.get_manager('CloudServiceStatsManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query_set_id', 'domain_id'])
    @append_query_filter(['query_set_id', 'provider', 'cloud_service_group', 'cloud_service_type',
                          'region_code', 'account', 'project_id', 'domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def list(self, params):
        """ List Cloud Service Stats
        Args:
            params (dict): {
                    'query_set_id': 'str',
                    'provider': 'str',
                    'cloud_service_group': 'str',
                    'cloud_service_type': 'str',
                    'region_code': 'str',
                    'account': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)',
                    'user_projects': 'list', // from meta
                }

        Returns:
            results (list)
            total_count (int)

        """
        query = params.get('query', {})
        query = self._change_project_group_filter(query, params['domain_id'])

        return self.cloud_svc_stats_mgr.list_cloud_service_stats(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'query.granularity', 'query.start', 'query.end', 'query.fields',
                     'query_set_id', 'domain_id'])
    @append_query_filter(['query_set_id', 'domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def analyze(self, params):
        """ Analyze Cloud Service Statistics Data
        Args:
            params (dict): {
                'query_set_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.TimeSeriesAnalyzeQuery)',
                'user_projects': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """
        domain_id = params['domain_id']
        query_set_id = params['query_set_id']
        query = params.get('query', {})

        query = self._change_project_group_filter(query, domain_id)

        return self.cloud_svc_stats_mgr.analyze_cloud_service_stats_by_granularity(query, domain_id, query_set_id)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['query_set_id', 'domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params):
        """ Get Cloud Service Statistics Data
        Args:
            params (dict): {
                'query_set_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        query = self._change_project_group_filter(query, params['domain_id'])

        return self.cloud_svc_stats_mgr.stat_cloud_service_stats(query)

    def _change_project_group_filter(self, query, domain_id):
        change_filter = []

        project_group_query = {
            'filter': [],
            'only': ['project_group_id']
        }

        for condition in query.get('filter', []):
            key = condition.get('key', condition.get('k'))
            value = condition.get('value', condition.get('v'))
            operator = condition.get('operator', condition.get('o'))

            if not all([key, operator]):
                raise ERROR_DB_QUERY(reason='filter condition should have key, value and operator.')

            if key == 'project_group_id':
                project_group_query['filter'].append(condition)
            else:
                change_filter.append(condition)

        if len(project_group_query['filter']) > 0:
            identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
            response = identity_mgr.list_project_groups(project_group_query, domain_id)
            project_group_ids = []
            project_ids = []
            for project_group_info in response.get('results', []):
                project_group_ids.append(project_group_info['project_group_id'])

            for project_group_id in project_group_ids:
                response = identity_mgr.list_projects_in_project_group(project_group_id, domain_id, True,
                                                                       {'only': ['project_id']})
                for project_info in response.get('results', []):
                    if project_info['project_id'] not in project_ids:
                        project_ids.append(project_info['project_id'])

            change_filter.append({'k': 'project_id', 'v': project_ids, 'o': 'in'})

        query['filter'] = change_filter
        return query
