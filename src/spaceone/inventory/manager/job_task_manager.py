import copy
import logging
import json
from typing import Tuple, Union
from jsonschema import validate
from datetime import datetime
from spaceone.core import config, queue
from spaceone.core.manager import BaseManager
from spaceone.core.scheduler.task_schema import SPACEONE_TASK_SCHEMA
from spaceone.core.model.mongo_model import QuerySet
from spaceone.inventory.manager.job_manager import JobManager
from spaceone.inventory.model.job_task_model import JobTask

_LOGGER = logging.getLogger(__name__)


class JobTaskManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_task_model: JobTask = self.locator.get_model("JobTask")

    def create_job_task(self, params: dict) -> JobTask:
        def _rollback(vo: JobTask):
            _LOGGER.info(f"[ROLLBACK] Delete job task: {vo.job_task_id}")
            vo.delete()

        job_task_vo: JobTask = self.job_task_model.create(params)
        self.transaction.add_rollback(_rollback, job_task_vo)
        return job_task_vo

    def get(
        self,
        job_task_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects: list = None,
    ) -> JobTask:
        conditions = {
            "job_task_id": job_task_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        return self.job_task_model.get(**conditions)

    def filter_job_tasks(self, **conditions) -> QuerySet:
        return self.job_task_model.filter(**conditions)

    def list(self, query: dict) -> Tuple[QuerySet, int]:
        return self.job_task_model.query(**query)

    def stat(self, query: dict) -> dict:
        return self.job_task_model.stat(**query)

    def push_job_task(self, params: dict) -> None:
        task = self.create_task_pipeline(copy.deepcopy(params))
        validate(task, schema=SPACEONE_TASK_SCHEMA)
        json_task = json.dumps(task)
        queue.put(self.get_queue_name(name="collect_queue"), json_task)

    @staticmethod
    def add_error(
        job_task_vo: JobTask,
        error_code: str,
        error_message: str,
        additional: dict = None,
    ) -> None:
        error_info = {"error_code": error_code, "message": str(error_message).strip()}

        if additional:
            error_info["additional"] = additional

        job_task_vo.append("errors", error_info)
        _LOGGER.error(
            f"[add_error] {job_task_vo.job_task_id}: {error_info}", exc_info=True
        )

    @staticmethod
    def _update_job_status_by_vo(
        job_task_vo: JobTask,
        status: str,
        started_at: datetime = None,
        finished_at: datetime = None,
        collecting_count_info: dict = None,
    ) -> None:
        params = {"status": status}

        if started_at:
            params["started_at"] = started_at

        if finished_at:
            params["finished_at"] = finished_at

        _LOGGER.debug(
            f"[update_job_status] job_task_id: {job_task_vo.job_task_id}, status: {status}"
        )
        job_task_vo = job_task_vo.update(params)

        if collecting_count_info:
            for key, value in collecting_count_info.items():
                if isinstance(value, int):
                    job_task_vo.increment(key, value)

    def make_inprogress_by_vo(
        self,
        job_task_vo: JobTask,
    ) -> None:
        if job_task_vo.status == "PENDING":
            self._update_job_status_by_vo(
                job_task_vo,
                "IN_PROGRESS",
                started_at=datetime.utcnow(),
            )

    def make_success_by_vo(
        self,
        job_task_vo: JobTask,
        collecting_count_info: dict = None,
    ) -> None:
        self._update_job_status_by_vo(
            job_task_vo,
            "SUCCESS",
            finished_at=datetime.utcnow(),
            collecting_count_info=collecting_count_info,
        )
        self.decrease_remained_sub_tasks(job_task_vo)

    def make_failure_by_vo(
        self,
        job_task_vo: JobTask,
        collecting_count_info: dict = None,
    ) -> None:
        self._update_job_status_by_vo(
            job_task_vo,
            "FAILURE",
            finished_at=datetime.utcnow(),
            collecting_count_info=collecting_count_info,
        )
        self.decrease_remained_sub_tasks(job_task_vo)

    def decrease_remained_sub_tasks(
        self, job_task_vo: JobTask, collecting_count_info: dict = None
    ) -> JobTask:
        job_task_vo: JobTask = job_task_vo.decrement("remained_sub_tasks")
        if job_task_vo.remained_sub_tasks == 0:
            job_mgr: JobManager = self.locator.get_manager(JobManager)
            if job_task_vo.status == "IN_PROGRESS":
                self.make_success_by_vo(job_task_vo, collecting_count_info)
                job_mgr.increase_success_tasks(
                    job_task_vo.job_id, job_task_vo.domain_id
                )
            else:
                job_mgr.increase_failure_tasks(
                    job_task_vo.job_id, job_task_vo.domain_id
                )
        return job_task_vo

    @staticmethod
    def delete_job_task_by_vo(job_task_vo: JobTask) -> None:
        job_task_vo.delete()

    @staticmethod
    def get_queue_name(name: str = "collect_queue") -> Union[str, None]:
        try:
            return config.get_global(name)
        except Exception as e:
            _LOGGER.warning(f"[_get_queue_name] name: {name} is not configured.")
            return None

    def create_task_pipeline(self, params: dict) -> dict:
        token = self.transaction.meta.get("token")
        params["token"] = token

        task = {
            "locator": "MANAGER",
            "name": "CollectingManager",
            "metadata": {},
            "method": "collecting_resources",
            "params": {"params": params},
        }

        stp = {
            "name": "collecting_resources",
            "version": "v1",
            "executionEngine": "BaseWorker",
            "stages": [task],
        }
        # _LOGGER.debug(f'[_create_task] tasks: {stp}')
        return stp
