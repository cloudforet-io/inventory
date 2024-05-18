import logging
from typing import Union, List

from spaceone.core.service import *
from spaceone.core.service.utils import *
from spaceone.core.error import *

from spaceone.inventory.model.metric.request import *
from spaceone.inventory.model.metric.response import *
from spaceone.inventory.manager.metric_manager import MetricManager
from spaceone.inventory.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class MetricService(BaseService):
    resource = "Metric"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metric_mgr = MetricManager()

    @transaction(
        permission="inventory:Metric.write",
        role_types=["WORKSPACE_OWNER"],
    )
    @convert_model
    def create(self, params: MetricCreateRequest) -> Union[MetricResponse, dict]:
        """Create metric

        Args:
            params (dict): {
                'metric_id': 'str',
                'name': 'str',                  # required
                'metric_type': 'str',           # required
                'resource_type': 'str',         # required
                'query_options': 'dict',        # required
                'unit': 'str',
                'tags': 'dict',
                'namespace_id': 'str',          # required
                'workspace_id': 'str',          # injected from auth (required)
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            MetricResponse:
        """

        metric_vo = self.metric_mgr.create_metric(params.dict())

        self.metric_mgr.analyze_resource(metric_vo, params.workspace_id)

        return MetricResponse(**metric_vo.to_dict())

    @transaction(
        permission="inventory:Metric.write",
        role_types=["WORKSPACE_OWNER"],
    )
    @convert_model
    def update(self, params: MetricUpdateRequest) -> Union[MetricResponse, dict]:
        """Update metric

        Args:
            params (dict): {
                'metric_id': 'str',             # required
                'name': 'str',
                'query_options': 'dict',
                'unit': 'str',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth (required)
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            MetricResponse:
        """

        metric_vo = self.metric_mgr.get_metric(
            params.metric_id,
            params.domain_id,
            params.workspace_id,
        )

        if metric_vo.is_managed:
            raise ERROR_PERMISSION_DENIED()

        if params.query_options:
            self.metric_mgr.analyze_resource(
                metric_vo, params.workspace_id, params.query_options
            )

        metric_vo = self.metric_mgr.update_metric_by_vo(
            params.dict(exclude_unset=True), metric_vo
        )

        return MetricResponse(**metric_vo.to_dict())

    @transaction(
        permission="inventory:Metric.write",
        role_types=["WORKSPACE_OWNER"],
    )
    @convert_model
    def delete(self, params: MetricDeleteRequest) -> None:
        """Delete metric

        Args:
            params (dict): {
                'metric_id': 'str',             # required
                'workspace_id': 'str',          # injected from auth (required)
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            None
        """

        metric_vo = self.metric_mgr.get_metric(
            params.metric_id,
            params.domain_id,
            params.workspace_id,
        )

        if metric_vo.is_managed:
            raise ERROR_PERMISSION_DENIED()

        self.metric_mgr.delete_metric_by_vo(metric_vo)

    @transaction(
        permission="inventory:Metric.write",
        role_types=["WORKSPACE_OWNER"],
    )
    @convert_model
    def run(self, params: MetricRunRequest) -> None:
        """Run query of metric

        Args:
            params (dict): {
                'metric_id': 'str',             # required
                'workspace_id': 'str',          # injected from auth (required)
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            None
        """

        metric_vo = self.metric_mgr.get_metric(
            params.metric_id,
            params.domain_id,
            params.workspace_id,
        )

        self.metric_mgr.run_metric_query(metric_vo)

    @transaction(
        permission="inventory:Metric.write",
        role_types=["WORKSPACE_OWNER"],
    )
    @convert_model
    def test(self, params: MetricTestRequest) -> dict:
        """Run query of metric

        Args:
            params (dict): {
                'metric_id': 'str',             # required
                'query_options': 'dict',
                'workspace_id': 'str',          # injected from auth (required)
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            dict: {
                'results': 'list',
                'more': 'bool'
            }
        """

        metric_vo = self.metric_mgr.get_metric(
            params.metric_id,
            params.domain_id,
            params.workspace_id,
        )

        results = self.metric_mgr.analyze_resource(
            metric_vo, params.workspace_id, params.query_options
        )

        return {"results": results, "more": False}

    @transaction(
        permission="inventory:Metric.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def get(self, params: MetricGetRequest) -> Union[MetricResponse, dict]:
        """Get metric

        Args:
            params (dict): {
                'metric_id': 'str',             # required
                'workspace_id': 'str',          # injected from auth (required)
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            MetricResponse:
        """

        metric_vo = self.metric_mgr.get_metric(
            params.metric_id,
            params.domain_id,
            params.workspace_id,
        )

        return MetricResponse(**metric_vo.to_dict())

    @transaction(
        permission="inventory:Metric.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @append_query_filter(
        [
            "metric_id",
            "metric_type",
            "resource_type",
            "is_managed",
            "namespace_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["metric_id", "name"])
    @convert_model
    def list(self, params: MetricSearchQueryRequest) -> Union[MetricsResponse, dict]:
        """List metrics

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'metric_id': 'str',
                'metric_type': 'str',
                'resource_type': 'str',
                'is_managed': 'bool',
                'workspace_id': 'list',         # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            MetricsResponse:
        """

        query = params.query or {}
        metric_vos, total_count = self.metric_mgr.list_metrics(query, params.domain_id)

        metrics_info = [metric_vo.to_dict() for metric_vo in metric_vos]
        return MetricsResponse(results=metrics_info, total_count=total_count)

    @transaction(
        permission="inventory:Metric.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(["metric_id", "name"])
    @convert_model
    def stat(self, params: MetricStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)', # required
                'workspace_id': 'list',     # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            dict: {
                'results': 'list',
                'total_count': 'int'
            }
        """

        query = params.query or {}
        return self.metric_mgr.stat_metrics(query)

    @transaction()
    def run_metric_query(self, params: dict) -> None:
        """Run metric query

        Args:
            params (dict): {
                'metric_id': 'str',
                'domain_id': 'str',
            }

        Returns:
            None
        """

        metric_id = params["metric_id"]
        domain_id = params["domain_id"]

        metric_vo = self.metric_mgr.get_metric(metric_id, domain_id)

        self.metric_mgr.run_metric_query(metric_vo, is_yesterday=True)

    @transaction()
    def run_all_metric_queries(self, params: dict) -> None:
        """Run all metric queries

        Args:
            params (dict): {}

        Returns:
            None
        """

        for domain_info in self._get_all_domains_info():
            domain_id = domain_info["domain_id"]
            try:
                self.run_metric_query_by_domain(domain_id)
            except Exception as e:
                _LOGGER.error(
                    f"[run_all_metric_queries] query error ({domain_id}): {e}",
                    exc_info=True,
                )

    def run_metric_query_by_domain(self, domain_id: str) -> None:
        self.metric_mgr.create_managed_metric(domain_id)
        metric_vos = self.metric_mgr.filter_metrics(domain_id=domain_id)

        for metric_vo in metric_vos:
            self.metric_mgr.push_task(metric_vo)

    @staticmethod
    def _get_all_domains_info() -> list:
        identity_mgr = IdentityManager()
        response = identity_mgr.list_domains({"only": ["domain_id"]})
        domains_info = response.get("results", [])

        _LOGGER.debug(f"[_get_all_domains_info] target domains: {len(domains_info)}")
        return domains_info
