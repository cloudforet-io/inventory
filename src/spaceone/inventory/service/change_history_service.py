from spaceone.core.service import *
from spaceone.inventory.manager.record_manager import RecordManager
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ChangeHistoryService(BaseService):
    resource = "ChangeHistory"

    def __init__(self, metadata):
        super().__init__(metadata)
        self.record_mgr: RecordManager = self.locator.get_manager("RecordManager")
        self.cloud_svc_mgr: CloudServiceManager = self.locator.get_manager(
            "CloudServiceManager"
        )

    @transaction(
        permission="inventory:ChangeHistory.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["cloud_service_id", "domain_id"])
    @append_query_filter(
        [
            "cloud_service_id",
            "record_id",
            "action",
            "user_id",
            "collector_id",
            "job_id",
            "updated_by",
            "domain_id",
        ]
    )
    @append_keyword_filter(["diff.key", "diff.before", "diff.after"])
    def list(self, params: dict):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',      # required
                    'query': 'dict (spaceone.api.core.v1.Query)',
                    'record_id': 'str',
                    'action': 'str',
                    'user_id': 'dict',
                    'collector_id': 'str',
                    'job_id': 'str',
                    'updated_by': 'str',
                    'workspace_id': 'str',          # injected from auth
                    'domain_id  ': 'str',           # injected from auth # required
                    'user_projects': 'list',        # injected from auth
                }

        Returns:
            results (list)
            total_count (int)
        """

        self.cloud_svc_mgr.get_cloud_service(
            params["cloud_service_id"],
            params["domain_id"],
            params.get("workspace_id"),
            params.get("user_projects"),
        )

        query = params.get("query", {})
        return self.record_mgr.list_records(query)

    @transaction(
        permission="inventory:ChangeHistory.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["cloud_service_id", "query", "domain_id"])
    @append_query_filter(["cloud_service_id", "domain_id"])
    @append_keyword_filter(["diff.key", "diff.before", "diff.after"])
    def stat(self, params: dict) -> dict:
        """
        Args:
            params (dict): {
                'cloud_service_id': 'str',      # required
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)', # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            values (list) : 'list of statistics data'
        """

        self.cloud_svc_mgr.get_cloud_service(
            params["cloud_service_id"],
            params["domain_id"],
            params.get("workspace_id"),
            params.get("user_projects"),
        )

        query = params.get("query", {})
        return self.record_mgr.stat_records(query)
