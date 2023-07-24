import logging
import json
from jsonschema import validate
from datetime import datetime, timedelta
from spaceone.core import config, queue
from spaceone.core.token import get_token
from spaceone.core.manager import BaseManager
from spaceone.core.scheduler.task_schema import SPACEONE_TASK_SCHEMA
from spaceone.inventory.manager.job_manager import JobManager
from spaceone.inventory.model.job_task_model import JobTask
from spaceone.inventory.lib.job_task_state import JobTaskStateMachine

_LOGGER = logging.getLogger(__name__)


class JobTaskManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_task_model: JobTask = self.locator.get_model('JobTask')

    def create_job_task(self, job_vo, domain_id, task_options):
        def _rollback(job_task_vo):
            _LOGGER.info(f'[ROLLBACK] Delete job_task: {job_task_vo.job_task_id}')
            job_task_vo.delete()

        params = {
            'job_id': job_vo.job_id,
            'collector_id': job_vo.collector_id,
            'domain_id': domain_id,
            'options': task_options
        }

        job_task_vo: JobTask = self.job_task_model.create(params)
        self.transaction.add_rollback(_rollback, job_task_vo)
        return job_task_vo

    def get(self, job_task_id, domain_id, only=None):
        return self.job_task_model.get(job_task_id=job_task_id, domain_id=domain_id, only=only)

    def list(self, query):
        return self.job_task_model.query(**query)

    def stat(self, query):
        return self.job_task_model.stat(**query)

    def delete(self, job_task_id, domain_id):
        job_task_vo = self.get(job_task_id, domain_id)
        job_task_vo.delete()

    def push_job_task(self, params):
        task = self.create_task_pipeline(params)
        validate(task, schema=SPACEONE_TASK_SCHEMA)
        json_task = json.dumps(task)
        queue.put(self.get_queue_name(name='collect_queue'), json_task)

    def check_duplicate_job_tasks(self, collector_id, secret_id, domain_id):
        started_at = datetime.utcnow() - timedelta(minutes=10)

        query = {
            'filter': [
                {'k': 'collector_id', 'v': collector_id, 'o': 'eq'},
                {'k': 'secret_id', 'v': secret_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'status', 'v': 'IN_PROGRESS', 'o': 'eq'},
                {'k': 'started_at', 'v': started_at, 'o': 'gte'},
            ]
        }

        job_task_vos, total_count = self.list(query)

        if total_count > 0:
            for job_task_vo in job_task_vos:
                _LOGGER.debug(f'[check_duplicate_job_tasks] Duplicate Job Info: {job_task_vo.job_id} '
                              f'({job_task_vo.collector_id})')
            return True

        return False

    def add_error(self, job_task_id, domain_id, error_code, msg, additional=None):
        job_mgr: JobManager = self.locator.get_manager(JobManager)

        error_info = {'error_code': error_code, 'message': str(msg).strip()}

        if additional:
            error_info['additional'] = additional

        job_task_vo = self.get(job_task_id, domain_id)
        job_task_vo.append('errors', error_info)
        job_mgr.mark_error(job_task_vo.job_id, domain_id)
        _LOGGER.error(f'[add_error] {job_task_id}: {error_info}', exc_info=True)

        return job_task_vo

    def _update_job_status(self, job_task_id, status, domain_id, started_at=None, finished_at=None, secret_info=None, collecting_count_info=None):
        job_task_vo = self.get(job_task_id, domain_id)
        params = {'status': status}

        if started_at:
            params['started_at'] = started_at

        if finished_at:
            params['finished_at'] = finished_at

        if secret_info:
            params.update(secret_info)

        if collecting_count_info:
            params.update(collecting_count_info)

        _LOGGER.debug(f'[update_job_status] job_task_id: {job_task_id}, status: {status}')
        return job_task_vo.update(params)

    def make_inprogress(self, job_task_id, domain_id, secret_info=None, collecting_count_info=None):
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.inprogress()
        self._update_job_status(job_task_id, job_state_machine.get_state(), domain_id,
                                started_at=datetime.utcnow(),
                                secret_info=secret_info,
                                collecting_count_info=collecting_count_info)

    def make_success(self, job_task_id, domain_id, secret_info=None, collecting_count_info=None):
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.success()
        self._update_job_status(job_task_id, job_state_machine.get_state(), domain_id,
                                finished_at=datetime.utcnow(),
                                secret_info=secret_info,
                                collecting_count_info=collecting_count_info)

    def make_failure(self, job_task_id, domain_id, secret_info=None, collecting_count_info=None):
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.failure()
        self._update_job_status(job_task_id, job_state_machine.get_state(), domain_id,
                                finished_at=datetime.utcnow(),
                                secret_info=secret_info,
                                collecting_count_info=collecting_count_info)

    def make_canceled(self, job_task_id, domain_id, secret_info=None, collecting_count_info=None):
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.canceled()
        self._update_job_status(job_task_id, job_state_machine.get_state(), domain_id,
                                finished_at=datetime.utcnow(),
                                secret_info=secret_info,
                                collecting_count_info=collecting_count_info)

    @staticmethod
    def delete_job_task_by_vo(job_task_vo):
        job_task_vo.delete()

    @staticmethod
    def get_queue_name(name='collect_queue'):
        try:
            return config.get_global(name)
        except Exception as e:
            _LOGGER.warning(f'[_get_queue_name] name: {name} is not configured')
            return None

    @staticmethod
    def create_task_pipeline(params):
        task = {
            'locator': 'MANAGER',
            'name': 'CollectingManager',
            'metadata': {'token': get_token(), 'domain_id': params.get('domain_id')},
            'method': 'collecting_resources',
            'params': {'params': params}
        }

        stp = {
            'name': 'collecting_resources',
            'version': 'v1',
            'executionEngine': 'BaseWorker',
            'stages': [task]
        }
        # _LOGGER.debug(f'[_create_task] tasks: {stp}')
        return stp
