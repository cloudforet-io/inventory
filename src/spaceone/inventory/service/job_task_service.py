from spaceone.core.service import *
from spaceone.inventory.model.job_task_model import JobTask
from spaceone.inventory.manager.job_task_manager import JobTaskManager


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class JobTaskService(BaseService):
    resource = "JobTask"

    def __init__(self, metadata):
        super().__init__(metadata)
        self.job_task_mgr: JobTaskManager = self.locator.get_manager("JobTaskManager")

    @transaction(
        permission="inventory:JobTask.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["job_task_id", "domain_id"])
    def delete(self, params: dict) -> None:
        """
        Args:
            params (dict): {
                'job_task_id': 'str',       # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth (required)
            }

        Returns:
            None
        """

        job_task_id = params["job_task_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        job_task_vo: JobTask = self.job_task_mgr.get(
            job_task_id, domain_id, workspace_id
        )
        self.job_task_mgr.delete_job_task_by_vo(job_task_vo)

    @transaction(
        permission="inventory:JobTask.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["job_task_id", "domain_id"])
    def get(self, params: dict) -> JobTask:
        """
        Args:
            params (dict): {
                'job_task_id': 'str',       # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list'     # injected from auth
            }

        Returns:
            job_task_vo (object)
        """

        return self.job_task_mgr.get(
            params["job_task_id"],
            params["domain_id"],
            params.get("workspace_id"),
            params.get("user_projects"),
        )

    @transaction(
        permission="inventory:JobTask.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "job_task_id",
            "status",
            "provider",
            "job_id",
            "secret_id",
            "service_account_id",
            "project_id",
            "workspace_id",
            "domain_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["job_task_id"])
    def list(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'job_task_id': 'str',
                'status': 'str',
                'job_id': 'str',
                'secret_id': 'str',
                'provider': 'str',
                'service_account_id': 'str',
                'project_id': 'str',
                'workspace_id': 'str',          # injected from auth
                'domain_id  ': 'str',           # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            results (list)
            total_count (int)
        """

        return self.job_task_mgr.list(params.get("query", {}))

    @transaction(
        permission="inventory:JobTask.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id", "user_projects"])
    @append_keyword_filter(["job_task_id"])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',     # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list',    # injected from auth
            }

        Returns:
            values (list) : 'list of statistics data'
        """

        query = params.get("query", {})
        return self.job_task_mgr.stat(query)
