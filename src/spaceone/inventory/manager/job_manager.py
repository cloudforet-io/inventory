import logging
from typing import Tuple
from datetime import datetime, timedelta
from spaceone.core.manager import BaseManager
from spaceone.core.model.mongo_model import QuerySet
from spaceone.inventory.error import *
from spaceone.inventory.lib.job_state import JobStateMachine
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

        job_params = params.copy()
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

    def increase_success_tasks(self, job_id: str, domain_id: str) -> Job:
        job_vo: Job = self.get_job(job_id, domain_id)
        return self.increase_success_tasks_by_vo(job_vo)

    def increase_failure_tasks(self, job_id, domain_id):
        job_vo: Job = self.get_job(job_id, domain_id)
        return self.increase_failure_tasks_by_vo(job_vo)

    def decrease_remained_tasks(self, job_id, domain_id):
        job_vo: Job = self.get_job(job_id, domain_id)
        return self.decrease_remained_tasks_by_vo(job_vo)

    def decrease_remained_tasks_by_vo(self, job_vo: Job) -> Job:
        job_vo = job_vo.decrement("remained_tasks")

        if job_vo.remained_tasks == 0 and job_vo.status != "CANCELED":
            if job_vo.mark_error:
                self.make_failure_by_vo(job_vo)
            else:
                self.make_success_by_vo(job_vo)

        if job_vo.remained_tasks < 0:
            _LOGGER.debug(
                f"[decrease_remained_tasks] {job_vo.job_id}, {job_vo.remained_tasks}"
            )
            raise ERROR_JOB_UPDATE(param="remained_tasks")

        return job_vo

    def add_error(
        self,
        job_id: str,
        domain_id: str,
        error_code: str,
        error_message: str,
        additional: dict = None,
    ) -> Job:
        """
        Args:
            job_id: str
            domain_id: str
            error_code: str
            error_message: str
            additional: dict

        Returns:
            job_vo: Job
        """

        if additional:
            error_message += f" ({additional})"

        _LOGGER.error(f"[add_error] Job Error({job_id}): {error_code} {error_message}")

        job_vo = self.get_job(job_id, domain_id)
        self.mark_error_by_vo(job_vo)

        return job_vo

    def update_job_timeout_by_hour(self, job_timeout: int, domain_id: str) -> None:
        created_at = datetime.utcnow() - timedelta(hours=job_timeout)
        query = {
            "filter": [
                {"k": "created_at", "v": created_at, "o": "lt"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "status", "v": "IN_PROGRESS", "o": "eq"},
            ]
        }

        job_vos, total_count = self.list_jobs(query)
        for job_vo in job_vos:
            self.make_failure_by_vo(job_vo)

    def get_duplicate_jobs(
        self, collector_id: str, domain_id: str, secret_id: str = None
    ) -> QuerySet:
        # created_at = datetime.utcnow() - timedelta(minutes=10)

        query = {
            "filter": [
                {"k": "collector_id", "v": collector_id, "o": "eq"},
                {"k": "secret_id", "v": secret_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "status", "v": "IN_PROGRESS", "o": "eq"},
                # {"k": "created_at", "v": created_at, "o": "gte"},
            ]
        }

        job_vos, total_count = self.list_jobs(query)
        return job_vos

    def make_success_by_vo(self, job_vo: Job) -> None:
        job_state_machine = JobStateMachine(job_vo)
        job_state_machine.success()
        self._update_job_status_by_vo(job_vo, job_state_machine.get_status())

    def make_failure_by_vo(self, job_vo: Job) -> None:
        job_state_machine = JobStateMachine(job_vo)
        job_state_machine.failure()
        self._update_job_status_by_vo(job_vo, job_state_machine.get_status())

    def make_canceled_by_vo(self, job_vo: Job) -> None:
        _LOGGER.debug(f"[make_canceled_by_vo] cancel job: {job_vo.job_id}")
        job_state_machine = JobStateMachine(job_vo)
        job_state_machine.canceled()
        self._update_job_status_by_vo(job_vo, job_state_machine.get_status())

    def check_cancel(self, job_id: str, domain_id: str) -> bool:
        job_vo = self.get_job(job_id, domain_id)
        job_state_machine = JobStateMachine(job_vo)
        job_status = job_state_machine.get_status()
        return job_status == "CANCELED"

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

    @staticmethod
    def increase_success_tasks_by_vo(job_vo: Job) -> Job:
        return job_vo.increment("success_tasks")

    @staticmethod
    def increase_failure_tasks_by_vo(job_vo: Job) -> Job:
        return job_vo.increment("failure_tasks")
