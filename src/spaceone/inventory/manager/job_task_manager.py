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
from spaceone.inventory.lib.job_task_state import JobTaskStateMachine

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
        task = self.create_task_pipeline(params)
        validate(task, schema=SPACEONE_TASK_SCHEMA)
        json_task = json.dumps(task)
        queue.put(self.get_queue_name(name="collect_queue"), json_task)

    def add_error(
        self,
        job_task_id: str,
        domain_id: str,
        error_code: str,
        error_message: str,
        additional: dict = None,
    ) -> JobTask:
        error_info = {"error_code": error_code, "message": str(error_message).strip()}

        if additional:
            error_info["additional"] = additional

        job_task_vo = self.get(job_task_id, domain_id)
        job_task_vo.append("errors", error_info)
        _LOGGER.error(f"[add_error] {job_task_id}: {error_info}", exc_info=True)

        job_mgr: JobManager = self.locator.get_manager(JobManager)
        job_mgr.mark_error(job_task_vo.job_id, domain_id)

        return job_task_vo

    def _update_job_status(
        self,
        job_task_id: str,
        status: str,
        domain_id: str,
        started_at: datetime = None,
        finished_at: datetime = None,
        collecting_count_info: dict = None,
    ) -> JobTask:
        job_task_vo = self.get(job_task_id, domain_id)
        params = {"status": status}

        if started_at:
            params["started_at"] = started_at

        if finished_at:
            params["finished_at"] = finished_at

        if collecting_count_info:
            params.update(collecting_count_info)

        _LOGGER.debug(
            f"[update_job_status] job_task_id: {job_task_id}, status: {status}"
        )
        return job_task_vo.update(params)

    def make_inprogress(
        self,
        job_task_id: str,
        domain_id: str,
        collecting_count_info: dict = None,
    ) -> None:
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.inprogress()
        self._update_job_status(
            job_task_id,
            job_state_machine.get_state(),
            domain_id,
            started_at=datetime.utcnow(),
            collecting_count_info=collecting_count_info,
        )

    def make_success(
        self,
        job_task_id: str,
        domain_id: str,
        collecting_count_info: dict = None,
    ) -> None:
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.success()
        self._update_job_status(
            job_task_id,
            job_state_machine.get_state(),
            domain_id,
            finished_at=datetime.utcnow(),
            collecting_count_info=collecting_count_info,
        )
        _LOGGER.debug(f"[make_success] job_task_id: {job_task_id}, status: SUCCESS")

    def make_failure(
        self,
        job_task_id: str,
        domain_id: str,
        collecting_count_info: dict = None,
    ) -> None:
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.failure()
        self._update_job_status(
            job_task_id,
            job_state_machine.get_state(),
            domain_id,
            finished_at=datetime.utcnow(),
            collecting_count_info=collecting_count_info,
        )
        _LOGGER.debug(f"[make_success] job_task_id: {job_task_id}, status: FAILURE")

    def make_canceled(
        self,
        job_task_id: str,
        domain_id: str,
        collecting_count_info: dict = None,
    ) -> None:
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.canceled()
        self._update_job_status(
            job_task_id,
            job_state_machine.get_state(),
            domain_id,
            finished_at=datetime.utcnow(),
            collecting_count_info=collecting_count_info,
        )
        _LOGGER.debug(f"[make_success] job_task_id: {job_task_id}, status: CANCLED")

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
        task = {
            "locator": "MANAGER",
            "name": "CollectingManager",
            "metadata": {"token": token},
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
