import logging
import pytz

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.inventory.model.cloud_service_report_model import CloudServiceReport
from spaceone.inventory.manager.cloud_service_report_manager import (
    CloudServiceReportManager,
)
from spaceone.inventory.manager.identity_manager import IdentityManager


_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ["report_id", "name"]


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceReportService(BaseService):
    resource = "CloudServiceReport"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_report_mgr: CloudServiceReportManager = self.locator.get_manager(
            "CloudServiceReportManager"
        )

    @transaction(
        permission="inventory:CloudServiceReport.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(
        [
            "name",
            "options",
            "schedule",
            "schedule.state",
            "target",
            "resource_group",
            "domain_id",
        ]
    )
    def create(self, params: dict) -> CloudServiceReport:
        """Create Cloud Service Report
        Args:
            params (dict): {
                'name': 'str',              # required
                'options': 'dict',          # required
                'file_format': 'str',
                'schedule': 'dict',         # required
                'target': 'dict',
                'timezone': 'str',
                'language': 'str',
                'tags': 'dict',
                'resource_group': 'str',    # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            cloud_service_report_vo (object)

        """

        identity_mgr: IdentityManager = self.locator.get_manager(IdentityManager)

        # Check permission by resource group
        if params["resource_group"] == "WORKSPACE":
            if "workspace_id" not in params:
                raise ERROR_REQUIRED_PARAMETER(key="workspace_id")

            identity_mgr.check_workspace(params["workspace_id"], params["domain_id"])
        else:
            params["workspace_id"] = "*"

        params["timezone"] = params.get("timezone", "UTC")
        params["language"] = params.get("language", "en")

        self._check_timezone(params["timezone"])
        self._check_language(params["language"])

        return self.cloud_svc_report_mgr.create_cloud_service_report(params)

    @transaction(
        permission="inventory:CloudServiceReport.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["report_id", "domain_id"])
    def update(self, params: dict) -> CloudServiceReport:
        """Update Cloud Service Report
        Args:
            params (dict): {
                'report_id': 'str',         # required
                'name': 'str',
                'options': 'dict',
                'file_format': 'str',
                'schedule': 'dict',
                'target': 'dict',
                'timezone': 'str',
                'language': 'str',
                'tags': 'dict',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            cloud_service_report_vo (object)
        """

        if "timezone" in params:
            self._check_timezone(params["timezone"])

        if "language" in params:
            self._check_language(params["language"])

        cloud_svc_report_vo: CloudServiceReport = (
            self.cloud_svc_report_mgr.get_cloud_service_report(
                params["report_id"], params["domain_id"], params.get("workspace_id")
            )
        )

        return self.cloud_svc_report_mgr.update_cloud_service_report_by_vo(
            params, cloud_svc_report_vo
        )

    @transaction(
        permission="inventory:CloudServiceReport.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["report_id", "domain_id"])
    def delete(self, params):
        """Delete Cloud Service Report
        Args:
            params (dict): {
                'report_id': 'str',     # required
                'workspace_id': 'str',  # injected from auth
                'domain_id': 'str',     # injected from auth (required)
            }

        Returns:
            None

        """

        cloud_svc_report_vo: CloudServiceReport = (
            self.cloud_svc_report_mgr.get_cloud_service_report(
                params["report_id"], params["domain_id"], params.get("workspace_id")
            )
        )

        self.cloud_svc_report_mgr.delete_cloud_service_report_by_vo(cloud_svc_report_vo)

    @transaction(
        permission="inventory:CloudServiceReport.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["report_id", "domain_id"])
    def send(self, params):
        """Send Report Manually
        Args:
            params (dict): {
                'report_id': 'str',     # required
                'workspace_id': 'str',  # injected from auth
                'domain_id': 'str',     # injected from auth (required)
            }

        Returns:
            None

        """

        cloud_svc_report_vo: CloudServiceReport = (
            self.cloud_svc_report_mgr.get_cloud_service_report(
                params["report_id"], params["domain_id"], params.get("workspace_id")
            )
        )

        self.cloud_svc_report_mgr.send_cloud_service_report(cloud_svc_report_vo)

    @transaction(
        permission="inventory:CloudServiceReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["report_id", "domain_id"])
    def get(self, params):
        """Get Cloud Service Report
        Args:
            params (dict): {
                'report_id': 'str',     # required
                'workspace_id': 'str',  # injected from auth
                'domain_id': 'str',     # injected from auth (required)
            }

        Returns:
            cloud_service_type_vo (object)
        """

        return self.cloud_svc_report_mgr.get_cloud_service_report(
            params["report_id"], params["domain_id"], params.get("workspace_id")
        )

    @transaction(
        permission="inventory:CloudServiceReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(["report_id", "name", "workspace_id", "domain_id"])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def list(self, params):
        """List Cloud Service Reports
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'report_id': 'str',
                'name': 'str',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            results (list)
            total_count (int)
        """

        query = params.get("query", {})
        return self.cloud_svc_report_mgr.list_cloud_service_reports(query)

    @transaction(
        permission="inventory:CloudServiceReport.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params):
        """Get Cloud Service Report Statistics
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',     # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            values (list) : 'list of statistics data'
        """

        query = params.get("query", {})
        return self.cloud_svc_report_mgr.stat_cloud_service_reports(query)

    @staticmethod
    def _check_timezone(timezone: str) -> None:
        if timezone not in pytz.all_timezones:
            raise ERROR_INVALID_PARAMETER(key="timezone", reason="Timezone is invalid.")

    @staticmethod
    def _check_language(language: str) -> None:
        if language not in ["en", "ko", "jp"]:
            raise ERROR_INVALID_PARAMETER(key="language", reason="Language is invalid.")
