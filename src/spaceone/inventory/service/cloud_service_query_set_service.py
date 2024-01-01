import logging

from spaceone.core import config
from spaceone.core.service import *
from spaceone.inventory.error import *
from spaceone.inventory.model.cloud_service_query_set_model import CloudServiceQuerySet
from spaceone.inventory.manager.cloud_service_query_set_manager import (
    CloudServiceQuerySetManager,
)
from spaceone.inventory.manager.identity_manager import IdentityManager


_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ["query_set_id", "name"]


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceQuerySetService(BaseService):
    resource = "CloudServiceQuerySet"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_query_set_mgr: CloudServiceQuerySetManager = (
            self.locator.get_manager("CloudServiceQuerySetManager")
        )

    @transaction(
        permission="inventory:CloudServiceQuerySet.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(
        [
            "name",
            "query_options",
            "query_options.fields",
            "provider",
            "cloud_service_group",
            "cloud_service_type",
            "resource_group",
            "domain_id",
        ]
    )
    def create(self, params: dict) -> CloudServiceQuerySet:
        """Create cloud service query set
        Args:
            params (dict): {
                'name': 'str',                  # required
                'query_options': 'dict',        # required
                'unit': 'dict',
                'provider': 'str',              # required
                'cloud_service_group': 'str',   # required
                'cloud_service_type': 'str',    # required
                'tags': 'dict',
                'resource_group': 'str',        # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            cloud_service_query_set_vo (object)

        """

        identity_mgr: IdentityManager = self.locator.get_manager(IdentityManager)

        # Check permission by resource group
        if params["resource_group"] == "WORKSPACE":
            if "workspace_id" not in params:
                raise ERROR_REQUIRED_PARAMETER(key="workspace_id")

            identity_mgr.check_workspace(params["workspace_id"], params["domain_id"])
        else:
            params["workspace_id"] = "*"

        params["query_type"] = "CUSTOM"

        return self.cloud_svc_query_set_mgr.create_cloud_service_query_set(params)

    @transaction(
        permission="inventory:CloudServiceQuerySet.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["query_set_id", "domain_id"])
    def update(self, params: dict) -> CloudServiceQuerySet:
        """Update cloud service query set
        Args:
            params (dict): {
                'query_set_id': 'str',          # required
                'name': 'str',
                'query_options': 'dict',
                'unit': 'dict',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            cloud_service_query_set_vo (object)

        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = (
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(
                params["query_set_id"], params["domain_id"], params.get("workspace_id")
            )
        )

        if cloud_svc_query_set_vo.query_type == "MANAGED":
            raise ERROR_NOT_ALLOWED_QUERY_TYPE()

        return self.cloud_svc_query_set_mgr.update_cloud_service_query_set_by_vo(
            params, cloud_svc_query_set_vo
        )

    @transaction(
        permission="inventory:CloudServiceQuerySet.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["query_set_id", "domain_id"])
    def delete(self, params: dict) -> None:
        """Delete cloud service query set
        Args:
            params (dict): {
                'query_set_id': 'str',      # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            None

        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = (
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(
                params["query_set_id"], params["domain_id"], params.get("workspace_id")
            )
        )

        if cloud_svc_query_set_vo.query_type == "MANAGED":
            raise ERROR_NOT_ALLOWED_QUERY_TYPE()

        self.cloud_svc_query_set_mgr.delete_cloud_service_query_set_by_vo(
            cloud_svc_query_set_vo
        )

    @transaction(
        permission="inventory:CloudServiceQuerySet.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["query_set_id", "domain_id"])
    def run(self, params: dict) -> None:
        """Run query set manually and save results
        Args:
            params (dict): {
                'query_set_id': 'str',      # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            None
        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = (
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(
                params["query_set_id"], params["domain_id"], params.get("workspace_id")
            )
        )

        self.cloud_svc_query_set_mgr.run_cloud_service_query_set(cloud_svc_query_set_vo)

    @transaction(
        permission="inventory:CloudServiceQuerySet.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["query_set_id", "domain_id"])
    def test(self, params: dict) -> dict:
        """Test query set manually
        Args:
            params (dict): {
                'query_set_id': 'str',      # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            values (list) : 'list of analyze data'
        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = (
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(
                params["query_set_id"], params["domain_id"], params.get("workspace_id")
            )
        )

        return self.cloud_svc_query_set_mgr.test_cloud_service_query_set(
            cloud_svc_query_set_vo
        )

    @transaction(
        permission="inventory:CloudServiceQuerySet.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["query_set_id", "domain_id"])
    def enable(self, params: dict) -> CloudServiceQuerySet:
        """Enable cloud service query set
        Args:
            params (dict): {
                'query_set_id': 'str',      # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            cloud_service_query_set_vo (object)
        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = (
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(
                params["query_set_id"], params["domain_id"], params.get("workspace_id")
            )
        )

        if cloud_svc_query_set_vo.query_type == "MANAGED":
            raise ERROR_NOT_ALLOWED_QUERY_TYPE()

        return self.cloud_svc_query_set_mgr.update_cloud_service_query_set_by_vo(
            {"state": "ENABLED"}, cloud_svc_query_set_vo
        )

    @transaction(
        permission="inventory:CloudServiceQuerySet.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["query_set_id", "domain_id"])
    def disable(self, params):
        """Disable cloud service query set
        Args:
            params (dict): {
                'query_set_id': 'str',      # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            cloud_service_query_set_vo (object)
        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = (
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(
                params["query_set_id"], params["domain_id"], params.get("workspace_id")
            )
        )

        if cloud_svc_query_set_vo.query_type == "MANAGED":
            raise ERROR_NOT_ALLOWED_QUERY_TYPE()

        return self.cloud_svc_query_set_mgr.update_cloud_service_query_set_by_vo(
            {"state": "DISABLED"}, cloud_svc_query_set_vo
        )

    @transaction(
        permission="inventory:CloudServiceQuerySet.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["query_set_id", "domain_id"])
    def get(self, params):
        """Get Cloud Service Query Set
        Args:
            params (dict): {
                'query_set_id': 'str',          # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            cloud_service_type_vo (object)
        """

        return self.cloud_svc_query_set_mgr.get_cloud_service_query_set(
            params["query_set_id"], params["domain_id"], params.get("workspace_id")
        )

    @transaction(
        permission="inventory:CloudServiceQuerySet.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "query_set_id",
            "name",
            "state",
            "query_type",
            "provider",
            "cloud_service_group",
            "cloud_service_type",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def list(self, params):
        """List cloud service query sets
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'query_set_id': 'str',
                'name': 'str',
                'state': 'str',
                'query_type': 'str',
                'provider': 'str',
                'cloud_service_group': 'str',
                'cloud_service_type': 'str',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            results (list)
            total_count (int)
        """

        query = params.get("query", {})
        return self.cloud_svc_query_set_mgr.list_cloud_service_query_sets(query)

    @transaction(
        permission="inventory:CloudServiceQuerySet.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params):
        """Get cloud service query set statistics
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',     # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            values (list) : 'list of statistics data'
        """

        query = params.get("query", {})
        return self.cloud_svc_query_set_mgr.stat_cloud_service_query_sets(query)

    @transaction()
    @check_required(["domain_id"])
    def run_query_sets_by_domain(self, params: dict) -> None:
        """Run cloud service query sets by domain_id

        Args:
            params (dict): {
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            None
        """

        domain_id = params["domain_id"]
        query_set_vos = self.cloud_svc_query_set_mgr.filter_cloud_service_query_sets(
            domain_id=domain_id
        )

        for query_set_vo in query_set_vos:
            self.cloud_svc_query_set_mgr.run_cloud_service_query_set(query_set_vo)

    @transaction()
    def run_all_query_sets(self, params: dict) -> None:
        """Run all cloud service query sets

        Args:
            params (dict): {}

        Returns:
            None
        """

        for domain_info in self._get_all_domains_info():
            domain_id = domain_info["domain_id"]
            try:
                self.cloud_svc_query_set_mgr.push_task(domain_id)
            except Exception as e:
                _LOGGER.error(
                    f"[run_query_sets_by_domain] query error({domain_id}): {e}",
                    exc_info=True,
                )

    def _get_all_domains_info(self) -> list:
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        response = identity_mgr.list_domains(
            {
                "only": ["domain_id"],
                "filter": [{"k": "state", "v": "ENABLED", "o": "eq"}],
            }
        )

        return response.get("results", [])
