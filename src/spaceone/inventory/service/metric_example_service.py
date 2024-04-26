import logging
from typing import Union, List

from spaceone.core.service import *
from spaceone.core.service.utils import *
from spaceone.core.error import *

from spaceone.inventory.model.metric_example.request import *
from spaceone.inventory.model.metric_example.response import *
from spaceone.inventory.manager.metric_example_manager import MetricExampleManager
from spaceone.inventory.manager.metric_manager import MetricManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class MetricExampleService(BaseService):
    resource = "MetricExample"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metric_example_mgr = MetricExampleManager()
        self.metric_mgr = MetricManager()

    @transaction(
        permission="inventory:MetricExample.write",
        role_types=["USER"],
    )
    @convert_model
    def create(
        self, params: MetricExampleCreateRequest
    ) -> Union[MetricExampleResponse, dict]:
        """Create metrix example

        Args:
            params (dict): {
                'metric_id': 'str',             # required
                'name': 'str',                  # required
                'options': 'dict',              # required
                'tags': 'dict',
                'user_id': 'str',               # injected from auth (required)
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            MetricExampleResponse:
        """

        metric_vo = self.metric_mgr.get_metric(
            params.metric_id, params.domain_id, params.workspace_id
        )

        params_dict = params.dict()
        params_dict["namespace_id"] = metric_vo.namespace_id

        metric_example_vo = self.metric_example_mgr.create_metric_example(params_dict)
        return MetricExampleResponse(**metric_example_vo.to_dict())

    @transaction(
        permission="inventory:MetricExample.write",
        role_types=["USER"],
    )
    @convert_model
    def update(
        self, params: MetricExampleUpdateRequest
    ) -> Union[MetricExampleResponse, dict]:
        """Update metric example

        Args:
            params (dict): {
                'example_id': 'str',            # required
                'name': 'str',
                'options': 'dict',
                'tags': 'dict',
                'user_id': 'str',               # injected from auth (required)
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            MetricExampleResponse:
        """

        metric_example_vo = self.metric_example_mgr.get_metric_example(
            params.example_id,
            params.domain_id,
            params.user_id,
            params.workspace_id,
        )

        metric_example_vo = self.metric_example_mgr.update_metric_example_by_vo(
            params.dict(exclude_unset=True), metric_example_vo
        )

        return MetricExampleResponse(**metric_example_vo.to_dict())

    @transaction(
        permission="inventory:MetricExample.write",
        role_types=["USER"],
    )
    @convert_model
    def delete(self, params: MetricExampleDeleteRequest) -> None:
        """Delete metric example

        Args:
            params (dict): {
                'example_id': 'str',            # required
                'user_id': 'str',               # injected from auth (required)
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            None
        """

        metric_example_vo = self.metric_example_mgr.get_metric_example(
            params.example_id,
            params.domain_id,
            params.user_id,
            params.workspace_id,
        )

        self.metric_example_mgr.delete_metric_example_by_vo(metric_example_vo)

    @transaction(
        permission="inventory:MetricExample.read",
        role_types=["USER"],
    )
    @convert_model
    def get(
        self, params: MetricExampleGetRequest
    ) -> Union[MetricExampleResponse, dict]:
        """Get metric example

        Args:
            params (dict): {
                'example_id': 'str',            # required
                'user_id': 'str',               # injected from auth (required)
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            MetricExampleResponse:
        """

        metric_example_vo = self.metric_example_mgr.get_metric_example(
            params.example_id,
            params.domain_id,
            params.user_id,
            params.workspace_id,
        )

        return MetricExampleResponse(**metric_example_vo.to_dict())

    @transaction(
        permission="inventory:MetricExample.read",
        role_types=["USER"],
    )
    @append_query_filter(
        [
            "example_id",
            "name",
            "metric_id",
            "namespace_id",
            "user_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["example_id", "name"])
    @convert_model
    def list(
        self, params: MetricExampleSearchQueryRequest
    ) -> Union[MetricExamplesResponse, dict]:
        """List metric examples

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'example_id': 'str',
                'name': 'str',
                'metric_id': 'str',
                'namespace_id': 'str',
                'user_id': 'str',               # injected from auth (required)
                'workspace_id': 'list',         # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            MetricExamplesResponse:
        """

        query = params.query or {}
        metric_example_vos, total_count = self.metric_example_mgr.list_metric_examples(
            query
        )

        metric_examples_info = [
            metric_example_vo.to_dict() for metric_example_vo in metric_example_vos
        ]
        return MetricExamplesResponse(
            results=metric_examples_info, total_count=total_count
        )

    @transaction(
        permission="inventory:MetricExample.read",
        role_types=["USER"],
    )
    @append_query_filter(["user_id", "workspace_id", "domain_id"])
    @append_keyword_filter(["example_id", "name"])
    @convert_model
    def stat(self, params: MetricExampleStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)', # required
                'user_id': 'str',           # injected from auth (required)
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
        return self.metric_example_mgr.stat_metric_examples(query)
