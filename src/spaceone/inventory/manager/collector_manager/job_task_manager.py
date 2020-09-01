# -*- coding: utf-8 -*-

import abc
import logging

from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.job_task_model import JobTask
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 2000

class JobTaskManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_task_model: JobTask = self.locator.get_model('JobTask')

    def create_job_task(self, job_vo, secret_info, domain_id):
        def _rollback(job_task_vo):
            _LOGGER.info(f'[ROLLBACK] Delete job_task: {job_task_vo.job_task_id}')
            job_task_vo.delete()

        params = {
            'job_id': job_vo.job_id,
            'domain_id': domain_id
        }
        params.update(secret_info)
        job_task_vo: JobTask = self.job_task_model.create(params)

        self.transaction.add_rollback(_rollback, job_task_vo)

        return job_task_vo

    def get(self, job_task_id, domain_id):
        return self.job_task_model.get(job_task_id=job_task_id, domain_id=domain_id)

    def list(self, query):
        return self.job_task_model.query(**query)

    def stat(self, query):
        return self.job_task_model.stat(**query)

    def delete(self, job_task_id, domain_id):
        job_task_vo = self.get(job_task_id, domain_id)
        job_task_vo.delete()

    def add_error(self, job_task_id, domain_id, error_code, msg, additional=None):
        message = repr(msg)
        error_info = {
            'error_code': error_code,
            'message': message[:MAX_MESSAGE_LENGTH]
        }
        if additional:
            error_info['additional'] = additional
        job_task_vo = self.get(job_task_id, domain_id)
        job_task_dict = job_task_vo.to_dict()
        errors = job_task_dict.get('errors', [])
        errors.append(error_info)
        params = {'errors': errors}
        _LOGGER.debug(f'[add_error] {params}')
        job_task_vo = job_task_vo.update(params)

        self.make_failure(job_task_id, domain_id)
        # Update Job Failure
        job_mgr = self.locator.get_manager('JobManager')
        job_mgr.mark_error(job_task_vo.job_id, domain_id)

        return job_task_vo

    #######################
    # Secret
    #######################
    def update_secret(self, job_task_id, secret_info, domain_id):
        job_task_vo = self.get(job_task_id, domain_id)
        return job_task_vo.update(secret_info)

    def update_stat(self, job_task_id, stat, domain_id):
        job_task_vo = self.get(job_task_id, domain_id)
        return job_task_vo.update(stat)

    #######################
    # State
    #######################
    def _update_job_status(self, job_task_id, status, domain_id, started_at=None, finished_at=None, secret=None, stat=None):
        """
        Args:
            secret(dict)
            stat(dict)
        """
        job_task_vo = self.get(job_task_id, domain_id)
        params = {'status': status}

        if started_at:
            params['started_at'] = started_at

        if finished_at:
            params['finished_at'] = finished_at

        if secret:
            params.update(secret)

        if stat:
            params.update(stat)

        _LOGGER.debug(f'[update_job_status] job_task_id: {job_task_id}, status: {status}')
        return job_task_vo.update(params)

    def update_created(self, job_task_id):
        job_task_vo = self.get(job_task_id, domain_id)
        #job_task_vo.update({'created': created
    def make_inprogress(self, job_task_id, domain_id, secret=None, stat=None):
        """ Make state to in-progress
        """
        job_task_vo = self.get(job_task_id, domain_id)
        # Update started_at automatically
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.inprogress()
        self._update_job_status(job_task_id, job_state_machine.get_state(), domain_id, started_at=datetime.utcnow(), secret=secret, stat=stat)

    def make_success(self, job_task_id, domain_id, secret=None, stat=None):
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.success()
        self._update_job_status(job_task_id, job_state_machine.get_state(), domain_id, finished_at=datetime.utcnow(), secret=secret, stat=stat)

    def make_failure(self, job_task_id, domain_id, secret=None, stat=None):
        job_task_vo = self.get(job_task_id, domain_id)
        job_state_machine = JobTaskStateMachine(job_task_vo)
        job_state_machine.failure()
        self._update_job_status(job_task_id, job_state_machine.get_state(), domain_id, finished_at=datetime.utcnow(), secret=secret, stat=stat)

PENDING = 'PENDING'
CANCELED = 'CANCELED'
INPROGRESS = 'IN_PROGRESS'
SUCCESS = 'SUCCESS'
FAILURE = 'FAILURE'

class JobTaskState(metaclass=abc.ABCMeta):
    def __init__(self):
        self.handle()

    @abc.abstractmethod
    def handle(self):
        pass

class PendingState(JobTaskState):
    def handle(self):
        pass

    def __str__(self):
        return PENDING

class CanceledState(JobTaskState):
    def handle(self):
        pass

    def __str__(self):
        return CANCELED

class InprogressState(JobTaskState):
    def handle(self):
        pass

    def __str__(self):
        return INPROGRESS

class SuccessState(JobTaskState):
    def handle(self):
        pass

    def __str__(self):
        return SUCCESS

class FailureState(JobTaskState):
    def handle(self):
        pass

    def __str__(self):
        return FAILURE

STATE_DIC = {
    'PENDING'       : PendingState(),
    'CANCELED'      : CanceledState(),
    'IN_PROGRESS'   : InprogressState(),
    'SUCCESS'       : SuccessState(),
    'FAILURE'       : FailureState()
}

class JobTaskStateMachine():
    def __init__(self, job_task_vo):
        self.job_task_id = job_task_vo.job_task_id
        self._state = STATE_DIC[job_task_vo.status]

    def inprogress(self):
        if isinstance(self._state, (PendingState, InprogressState, SuccessState, FailureState)):
            self._state = InprogressState()
        else:
            raise ERROR_JOB_STATE_CHANGE(action='inprogress', job_task_id=self.job_task_id, state=str(self._state))
        return self.get_state()

    def success(self):
        if isinstance(self._state, (InprogressState)):
            self._state = SuccessState()
        else:
            raise ERROR_JOB_STATE_CHANGE(action='success', job_task_id=self.job_task_id, state=str(self._state))
        return self.get_state()

    def failure(self):
        self._state = FailureState()
        return self.get_state()

    def get_state(self):
        return str(self._state)
