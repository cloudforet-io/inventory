import logging
from typing import Union, List

from spaceone.core.service import *
from spaceone.core.service.utils import *
from spaceone.core.error import *

from spaceone.inventory.model.metric_data.request import *
from spaceone.inventory.model.metric_data.response import *
from spaceone.inventory.manager.metric_manager import MetricManager
from spaceone.inventory.manager.metric_data_manager import MetricDataManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class MetricDataService(BaseService):
    resource = "MetricData"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metric_mgr = MetricManager()
        self.metric_data_mgr = MetricDataManager()

    @transaction(
        permission="inventory:MetricData.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(
        [
            "metric_id",
            "project_id",
            "workspace_id",
            "domain_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["metric_id", "name"])
    @set_query_page_limit(1000)
    @convert_model
    def list(
        self, params: MetricDataSearchQueryRequest
    ) -> Union[MetricDatasResponse, dict]:
        """List metric data
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'metric_id': 'str',             # required
                'project_id': 'bool',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            MetricDataResponse:
        """

        metric_id = params.metric_id
        domain_id = params.domain_id
        query = params.query or {}

        self.metric_mgr.check_and_run_metric_query(
            metric_id, domain_id, params.workspace_id
        )

        metric_data_vos, total_count = self.metric_data_mgr.list_metric_data(query)

        metric_datas_info = [
            metric_data_vo.to_dict() for metric_data_vo in metric_data_vos
        ]
        return MetricDatasResponse(results=metric_datas_info, total_count=total_count)

    @transaction(
        permission="inventory:MetricData.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(["metric_id", "workspace_id", "domain_id", "user_projects"])
    @append_keyword_filter(["metric_id", "name"])
    @set_query_page_limit(1000)
    @convert_model
    def analyze(self, params: MetricDataAnalyzeQueryRequest) -> dict:
        """Analyze metric data
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.AnalyzeQuery)',    # required
                'metric_id': 'str',             # required
                'workspace_id': 'list',         # injected from auth
                'domain_id': 'str',             # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            dict: {
                'results': 'list',
                'more': 'bool'
            }
        """

        domain_id = params.domain_id
        metric_id = params.metric_id
        query = params.query or {}

        self.metric_mgr.check_and_run_metric_query(
            metric_id, domain_id, params.workspace_id
        )

        return self.metric_data_mgr.analyze_metric_data_by_granularity(
            query, domain_id, metric_id
        )

    @transaction(
        permission="inventory:MetricData.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(["metric_id", "workspace_id", "domain_id", "user_projects"])
    @append_keyword_filter(["metric_id", "name"])
    @set_query_page_limit(1000)
    @convert_model
    def stat(self, params: MetricDataStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)', # required
                'workspace_id': 'list',     # injected from auth
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list',    # injected from auth
            }

        Returns:
            dict: {
                'results': 'list',
                'total_count': 'int'
            }
        """

        query = params.query or {}

        return self.metric_data_mgr.stat_metric_data(query)

    @staticmethod
    def _check_required(query: dict) -> None:
        for key in ["granularity", "start", "end", "fields"]:
            if key not in query:
                raise ERROR_REQUIRED_PARAMETER(key=key)
