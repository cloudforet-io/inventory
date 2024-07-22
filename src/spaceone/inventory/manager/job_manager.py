import logging
from typing import Tuple, List
from datetime import datetime, timedelta
from spaceone.core import cache, config
from spaceone.core.manager import BaseManager
from spaceone.core.model.mongo_model import QuerySet
from spaceone.inventory.error import *
from spaceone.inventory.model.collector_model import Collector
from spaceone.inventory.model.job_model import Job
from spaceone.inventory.model.job_task_model import JobTask
from spaceone.inventory.manager.metric_manager import MetricManager
from spaceone.inventory.manager.metric_data_manager import MetricDataManager

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

        if job_vo.remained_tasks == 0:
            if job_vo.status == "IN_PROGRESS":
                if job_vo.failure_tasks > 0:
                    self.make_failure_by_vo(job_vo)
                else:
                    self.make_success_by_vo(job_vo)

            if self._is_changed(job_vo):
                self._run_metric_queries(job_vo.plugin_id, job_vo.domain_id)

    def _is_changed(self, job_vo: Job) -> bool:
        job_task_model: JobTask = self.locator.get_model("JobTask")
        job_task_vos: List[JobTask] = job_task_model.filter(
            job_id=job_vo.job_id, domain_id=job_vo.domain_id
        )
        is_changed = False

        for job_task_vo in job_task_vos:
            if (
                job_task_vo.created_count > 0
                or job_task_vo.updated_count > 0
                or job_task_vo.deleted_count > 0
            ):
                is_changed = True
                break

        _LOGGER.debug(
            f"[_is_changed] job_id: {job_vo.job_id}, is_changed: {is_changed}"
        )
        return is_changed

    def _run_metric_queries(self, plugin_id: str, domain_id: str) -> None:
        metric_mgr = MetricManager()
        recent_metrics = self._get_recent_metrics(domain_id)

        managed_metric_vos = metric_mgr.filter_metrics(
            is_managed=True, domain_id=domain_id, plugin_id=None
        )
        for managed_metric_vo in managed_metric_vos:
            if managed_metric_vo.is_new or (
                managed_metric_vo.metric_id in recent_metrics
            ):
                metric_mgr.push_task(managed_metric_vo)

        plugin_metric_vos = metric_mgr.filter_metrics(
            is_managed=True, plugin_id=plugin_id, domain_id=domain_id
        )
        for plugin_metric_vo in plugin_metric_vos:
            if plugin_metric_vo.is_new or (
                plugin_metric_vo.metric_id in recent_metrics
            ):
                metric_mgr.push_task(plugin_metric_vo)

    @staticmethod
    def _get_recent_metrics(domain_id: str) -> List[str]:
        metric_data_mgr = MetricDataManager()
        metric_cache_ttl = config.get_global("METRIC_QUERY_TTL", 3)
        ttl_time = datetime.utcnow() - timedelta(days=metric_cache_ttl)

        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "updated_at", "v": ttl_time, "o": "gte"},
            ]
        }

        _LOGGER.debug(
            f"[_get_metric_query_history] metric_cache_ttl: {metric_cache_ttl} days"
        )

        history_vos, total_count = metric_data_mgr.list_metric_query_history(query)
        recent_metrics = []
        for history_vo in history_vos:
            recent_metrics.append(history_vo.metric_id)

        _LOGGER.debug(
            f"[_get_metric_query_history] recent_metrics({domain_id}): {recent_metrics}"
        )

        return recent_metrics

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

    @staticmethod
    def _update_job_status_by_vo(job_vo: Job, status: str) -> Job:
        params = {"status": status}
        if status in ["SUCCESS", "FAILURE", "CANCELED"]:
            params.update({"finished_at": datetime.utcnow()})

        _LOGGER.debug(f"[update_job_status] job_id: {job_vo.job_id}, status: {status}")
        return job_vo.update(params)
