from spaceone.core.service import *
from spaceone.inventory.model.job_model import Job
from spaceone.inventory.manager.job_manager import JobManager
from spaceone.inventory.manager.job_task_manager import JobTaskManager


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class JobService(BaseService):
    resource = "Job"

    def __init__(self, metadata):
        super().__init__(metadata)
        self.job_mgr: JobManager = self.locator.get_manager("JobManager")

    @transaction(
        permission="inventory:Job.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["job_id", "domain_id"])
    def delete(self, params):
        """
        Args:
            params (dict): {
                'job_id': 'str',        # required
                'workspace_id': 'str',  # injected from auth
                'domain_id': 'str'      # injected from auth (required)
            }

        Returns:
            None
        """
        job_id = params["job_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        job_vo: Job = self.job_mgr.get_job(job_id, domain_id, workspace_id)

        job_task_mgr: JobTaskManager = self.locator.get_manager("JobTaskManager")

        job_task_vos = job_task_mgr.filter_job_tasks(job_id=job_id, domain_id=domain_id)
        job_task_vos.delete()

        self.job_mgr.delete_job_by_vo(job_vo)

    @transaction(
        permission="inventory:Job.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["job_id", "domain_id"])
    def get(self, params):
        """
        Args:
            params (dict): {
                'job_id': 'str',        # required
                'workspace_id': 'str',  # injected from auth
                'domain_id': 'str',     # injected from auth (required)
            }

        Returns:
            job_vo (object)
        """

        return self.job_mgr.get_job(
            params["job_id"], params["domain_id"], params.get("workspace_id")
        )

    @transaction(
        permission="inventory:Job.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "job_id",
            "status",
            "collector_id",
            "plugin_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["job_id"])
    @set_query_page_limit(1000)
    def list(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'job_id': 'str',
                'status': 'str',
                'collector_id': 'dict',
                'plugin_id': 'str',
                'workspace_id': 'str',      # injected from auth
                'domain_id  ': 'str',       # injected from auth (required)
            }

        Returns:
            results (list)
            total_count (int)
        """

        query = params.get("query", {})
        return self.job_mgr.list_jobs(query)

    @transaction(
        permission="inventory:Job.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["query", "query.fields", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(["job_id"])
    @set_query_page_limit(1000)
    def analyze(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.AnalyzeQuery)',    # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            values (list) : 'list of analyze data'
        """

        query = params.get("query", {})
        return self.job_mgr.analyze_jobs(query)

    @transaction(
        permission="inventory:Job.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(["job_id"])
    @set_query_page_limit(1000)
    def stat(self, params):
        """
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
        return self.job_mgr.stat_jobs(query)
