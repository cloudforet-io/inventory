import logging
from typing import Tuple
from datetime import datetime, timedelta
from spaceone.core import cache
from spaceone.core.manager import BaseManager
from spaceone.core.model.mongo_model import QuerySet
from spaceone.inventory.error import *
from spaceone.inventory.model.collector_model import Collector
from spaceone.inventory.model.job_model import Job

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_model: Job = self.locator.get_model("Job")

    def create_job(self, collector_vo: Collector, params: dict) -> Job:
        """Create Job for collect method
        Args:
            collector_vo: collector model
            params(dict): {
                'secret_id': str,
            }
        Returns: job_vo
        """

        request_workspace_id = params.get("workspace_id")
        changed_request_workspace_id = None
        if isinstance(request_workspace_id, list):
            for workspace_id in request_workspace_id:
                if workspace_id != "*":
                    changed_request_workspace_id = workspace_id
        else:
            changed_request_workspace_id = request_workspace_id

        job_params = params.copy()
        job_params["request_secret_id"] = params.get("secret_id")
        job_params["request_workspace_id"] = changed_request_workspace_id
        job_params["collector"] = collector_vo
        job_params["collector_id"] = collector_vo.collector_id
        job_params["resource_group"] = collector_vo.resource_group
        job_params["workspace_id"] = collector_vo.workspace_id
        job_params["domain_id"] = collector_vo.domain_id

        return self.job_model.create(job_params)

    @staticmethod
    def update_job_by_vo(params: dict, job_vo: Job) -> Job:
        return job_vo.update(params)

    @staticmethod
    def delete_job_by_vo(job_vo: Job) -> None:
        job_vo.delete()

    def get_job(self, job_id: str, domain_id: str, workspace_id: str = None) -> Job:
        conditions = {
            "job_id": job_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions.update({"workspace_id": workspace_id})

        return self.job_model.get(**conditions)

    def filter_jobs(self, **conditions) -> QuerySet:
        return self.job_model.filter(**conditions)

    def list_jobs(self, query: dict) -> Tuple[QuerySet, int]:
        return self.job_model.query(**query)

    def analyze_jobs(self, query: dict) -> dict:
        return self.job_model.analyze(**query)

    def stat_jobs(self, query: dict) -> dict:
        return self.job_model.stat(**query)

    def increase_success_tasks(self, job_id: str, domain_id: str) -> None:
        job_vo: Job = self.get_job(job_id, domain_id)
        job_vo.increment("success_tasks")
        self.decrease_remained_tasks_by_vo(job_vo)

    def increase_failure_tasks(self, job_id: str, domain_id: str) -> None:
        job_vo: Job = self.get_job(job_id, domain_id)
        job_vo.increment("failure_tasks")
        self.decrease_remained_tasks_by_vo(job_vo)

    def decrease_remained_tasks_by_vo(self, job_vo: Job) -> None:
        job_vo = job_vo.decrement("remained_tasks")

        if job_vo.remained_tasks == 0 and job_vo.status != "CANCELED":
            if job_vo.status == "IN_PROGRESS":
                self.make_success_by_vo(job_vo)

            self._delete_metric_cache(job_vo.plugin_id, job_vo.domain_id)

    @staticmethod
    def _delete_metric_cache(plugin_id: str, domain_id: str) -> None:
        cache.delete_pattern(f"inventory:managed-metric:{domain_id}:*:load")
        cache.delete_pattern(f"inventory:plugin-metric:{domain_id}:{plugin_id}:*:load")

    def update_job_timeout_by_hour(self, job_timeout: int, domain_id: str) -> None:
        created_at = datetime.utcnow() - timedelta(hours=job_timeout)
        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "created_at", "v": created_at, "o": "lt"},
                {"k": "status", "v": "IN_PROGRESS", "o": "eq"},
            ]
        }

        job_vos, total_count = self.list_jobs(query)
        for job_vo in job_vos:
            self.make_failure_by_vo(job_vo)

    def get_duplicate_jobs(
        self,
        collector_id: str,
        domain_id: str,
        request_workspace_id: str = None,
        request_secret_id: str = None,
    ) -> QuerySet:
        changed_request_workspace_id = None
        if isinstance(request_workspace_id, list):
            for workspace_id in request_workspace_id:
                if workspace_id != "*":
                    changed_request_workspace_id = workspace_id
        else:
            changed_request_workspace_id = request_workspace_id

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "collector_id", "v": collector_id, "o": "eq"},
                {"k": "status", "v": "IN_PROGRESS", "o": "eq"},
                {
                    "k": "request_workspace_id",
                    "v": changed_request_workspace_id,
                    "o": "eq",
                },
                {"k": "request_secret_id", "v": request_secret_id, "o": "eq"},
            ]
        }

        job_vos, total_count = self.list_jobs(query)
        return job_vos

    def make_success_by_vo(self, job_vo: Job) -> None:
        self._update_job_status_by_vo(job_vo, "SUCCESS")

    def make_failure_by_vo(self, job_vo: Job) -> None:
        self._update_job_status_by_vo(job_vo, "FAILURE")

    def make_canceled_by_vo(self, job_vo: Job) -> None:
        _LOGGER.debug(f"[make_canceled_by_vo] cancel job: {job_vo.job_id}")
        self._update_job_status_by_vo(job_vo, "CANCELED")

    def check_cancel(self, job_id: str, domain_id: str) -> bool:
        job_vo: Job = self.get_job(job_id, domain_id)
        return job_vo.status == "CANCELED"

    def mark_error(self, job_id: str, domain_id: str) -> None:
        job_vo = self.get_job(job_id, domain_id)
        self.mark_error_by_vo(job_vo)

    def mark_error_by_vo(self, job_vo: Job) -> None:
        self.update_job_by_vo({"mark_error": 1}, job_vo)

    @staticmethod
    def _update_job_status_by_vo(job_vo: Job, status: str) -> Job:
        params = {"status": status}
        if status in ["SUCCESS", "FAILURE", "CANCELED"]:
            params.update({"finished_at": datetime.utcnow()})

        _LOGGER.debug(f"[update_job_status] job_id: {job_vo.job_id}, status: {status}")
        return job_vo.update(params)
