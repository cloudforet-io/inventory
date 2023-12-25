from spaceone.core.service import *
from spaceone.inventory.manager.cloud_service_stats_manager import (
    CloudServiceStatsManager,
)


_KEYWORD_FILTER = ["query_set_id", "name"]


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceStatsService(BaseService):
    resource = "CloudServiceStats"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_stats_mgr: CloudServiceStatsManager = self.locator.get_manager(
            "CloudServiceStatsManager"
        )

    @transaction(
        permission="inventory:CloudServiceStats.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query_set_id", "domain_id"])
    @append_query_filter(
        [
            "query_set_id",
            "provider",
            "cloud_service_group",
            "cloud_service_type",
            "region_code",
            "account",
            "project_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def list(self, params):
        """List cloud service statistics data
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'query_set_id': 'str',              # required
                'provider': 'str',
                'cloud_service_group': 'str',
                'cloud_service_type': 'str',
                'region_code': 'str',
                'account': 'str',
                'project_id': 'str',
                'workspace_id': 'str',              # injected from auth
                'domain_id': 'str',                 # injected from auth (required)
                'user_projects': 'list',            # injected from auth
            }

        Returns:
            results (list)
            total_count (int)
        """

        query = params.get("query", {})
        return self.cloud_svc_stats_mgr.list_cloud_service_stats(query)

    @transaction(
        permission="inventory:CloudServiceStats.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(
        [
            "query_set_id",
            "query",
            "query.granularity",
            "query.start",
            "query.end",
            "query.fields",
            "domain_id",
        ]
    )
    @append_query_filter(["query_set_id", "workspace_id", "domain_id", "user_projects"])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def analyze(self, params):
        """Analyze cloud service statistics data
        Args:
            params (dict): {
                'query_set_id': 'str',              # required
                'query': 'dict (spaceone.api.core.v1.TimeSeriesAnalyzeQuery)',  # required
                'workspace_id': 'str',              # injected from auth
                'domain_id': 'str',                 # injected from auth (required)
                'user_projects': 'list',            # injected from auth
            }

        Returns:
            values (list) : 'list of statistics data'
        """

        domain_id = params["domain_id"]
        query_set_id = params["query_set_id"]
        query = params.get("query", {})

        return self.cloud_svc_stats_mgr.analyze_cloud_service_stats_by_granularity(
            query, domain_id, query_set_id
        )

    @transaction(
        permission="inventory:CloudServiceStats.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id", "user_projects"])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def stat(self, params):
        """Get cloud service statistics data
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',     # required
                'workspace_id': 'str',              # injected from auth
                'domain_id': 'str',                 # injected from auth (required)
                'user_projects': 'list',            # injected from auth
            }

        Returns:
            values (list) : 'list of statistics data'
        """

        query = params.get("query", {})
        return self.cloud_svc_stats_mgr.stat_cloud_service_stats(query)
