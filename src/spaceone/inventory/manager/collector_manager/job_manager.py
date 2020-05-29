# -*- coding: utf-8 -*-

import abc
import logging

from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.job_model import Job
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_model: Job = self.locator.get_model('Job')

    def list_jobs(self, query):
        return self.job_model.query(**query)

    def stat_jobs(self, query):
        return self.job_model.stat(**query)

    def get(self, job_id, domain_id):
        return self.job_model.get(job_id=job_id, domain_id=domain_id)

    def delete(self, job_id, domain_id):
        job_vo = self.get(job_id, domain_id)
        job_vo.delete()

    def delete_by_collector_id(self, collector_id, domain_id):
        query = {'filter': [
                        {'k': 'collector_id', 'v': collector_id, 'o': 'eq'},
                        {'k': 'domain_id', 'v': domain_id, 'o': 'eq'}
                    ]
                }
        jobs, total_count = self.list_jobs(query)
        for job in jobs:
            job.delete()

    def create_job(self, collector_vo, params):
        """ Create Job for collect method
        Args:
            collector_vo: collector model
            params(dict): {
                'collector_id': str,
                'filter': dict,
                'secret_id': str,
                'collect_mode': str,
                'domain_id': str
                }
        Returns: job_vo
        """
        job_params = params.copy()
        job_params['collector'] = collector_vo
        job_params = self._check_filter(job_params)

        _LOGGER.debug(f'[create_job] params: {job_params}')
        job_vo = self.job_model.create(job_params)

        return job_vo

    def increase_remained_tasks(self, job_id, domain_id):
        job_vo = self.get(job_id, domain_id)
        remained_tasks = job_vo.remained_tasks + 1
        params = {'remained_tasks': remained_tasks}
        _LOGGER.debug(f'[increase_remained_tasks] {job_id}, {params}')
        return job_vo.update(params)

    def decrease_remained_tasks(self, job_id, domain_id):
        job_vo = self.get(job_id, domain_id)
        remained_tasks = job_vo.remained_tasks - 1
        params = {'remained_tasks': remained_tasks}
        _LOGGER.debug(f'[decrease_remained_tasks] {job_id}, {params}')
        job_vo = job_vo.update(params)

        if remained_tasks == 0:
            # Update to Finished
            self.make_finished(job_id, domain_id)

        if remained_tasks < 0:
            _LOGGER.debug(f'[decrease_remained_tasks] {job_id}, {remained_tasks}')
            raise ERROR_JOB_UPDATE(param='remained_tasks')
        return job_vo

    def update_job_state_by_hour(self, hour, state, domain_id):
        # Find Jobs
        created_at = datetime.utcnow() - timedelta(hours=hour)
        query = {'filter': [{'k': 'created_at', 'v': created_at, 'o': 'lt'},
                            {'k': 'domain_id',  'v': domain_id, 'o': 'eq'},
                            {'k': 'state',      'v': 'IN_PROGRESS', 'o': 'eq'}]
                 }
        jobs, total_count = self.list_jobs(query)
        _LOGGER.debug(f'[update_job_state_by_hour] job count: {total_count} to {state}')
        for job in jobs:
            self.make_failure(job.job_id, domain_id)

    def _update_job_state(self, job_id, state, domain_id):
        job_vo = self.get(job_id, domain_id)
        params = {'state': state}
        _LOGGER.debug(f'[update_job_state] job_id: {job_id}, state: {state}')
        return job_vo.update(params)

    def make_inprgress(self, job_id, domain_id):
        """ Make state to in-progress
        """
        job_vo = self.get(job_id, domain_id)
        job_state_machine = JobStateMachine(job_vo)
        job_state_machine.inprogress()
        self._update_job_state(job_id, job_state_machine.get_state(), domain_id)

    def make_finished(self, job_id, domain_id):
        job_vo = self.get(job_id, domain_id)
        job_state_machine = JobStateMachine(job_vo)
        job_state_machine.finished()
        self._update_job_state(job_id, job_state_machine.get_state(), domain_id)

    def make_canceled(self, job_id, domain_id):
        job_vo = self.get(job_id, domain_id)
        job_state_machine = JobStateMachine(job_vo)
        job_state_machine.canceled()
        self._update_job_state(job_id, job_state_machine.get_state(), domain_id)

    def make_failure(self, job_id, domain_id):
        job_vo = self.get(job_id, domain_id)
        job_state_machine = JobStateMachine(job_vo)
        job_state_machine.failure()
        self._update_job_state(job_id, job_state_machine.get_state(), domain_id)


    def increase_created_count(self, job_id, domain_id):
        """ Increase created_count field
        """
        job_vo = self.get(job_id, domain_id)

        created_count = job_vo.created_count + 1

        updated_param = {
            'created_count': created_count,
            'last_updated_at': datetime.utcnow()
        }
        return job_vo.update(updated_param)

    def increase_updated_count(self, job_id, domain_id):
        """ Increase updated_count field
        """
        job_vo = self.get(job_id, domain_id)

        updated_count = job_vo.updated_count + 1

        updated_param = {
            'updated_count': updated_count,
            'last_updated_at': datetime.utcnow()
        }
        return job_vo.update(updated_param)

    def is_canceled(self, job_id, domain_id):
        """ Return True/False
        """
        job_vo = self.get(job_id, domain_id)
        job_state_machine = JobStateMachine(job_vo)
        if job_state_machine.get_state()  == CANCELED:
            return True
        return False

    def _check_filter(self, params):
        """ Schedule request may have filter
        Change filter -> filters, since mongodb does not support filter as member key
        """
        if 'filter' in params:
            params['filters'] = params['filter']
            del params['filter']
        return params

CREATED = 'CREATED'
INPROGRESS = 'IN_PROGRESS'
CANCELED = 'CANCELED'
FINISHED = 'FINISHED'
FAILURE = 'FAILURE'
TIMEOUT = 'TIMEOUT'

class JobState(metaclass=abc.ABCMeta):
    def __init__(self):
        self.handle()

    @abc.abstractmethod
    def handle(self):
        pass

class InprogressState(JobState):
    def handle(self):
        pass

    def __str__(self):
        return INPROGRESS

class CreatedState(JobState):
    def handle(self):
        pass

    def __str__(self):
        return CREATED

class CanceledState(JobState):
    def handle(self):
        pass

    def __str__(self):
        return CANCELED

class FinishedState(JobState):
    def handle(self):
        pass

    def __str__(self):
        return FINISHED

class FailureState(JobState):
    def handle(self):
        pass

    def __str__(self):
        return FAILURE

class TimeoutState(JobState):
    def handle(self):
        pass

    def __str__(self):
        return TIMEOUT

STATE_DIC = {
    'CREATED'       : CreatedState(),
    'IN_PROGRESS'   : InprogressState(),
    'CANCELED'      : CanceledState(),
    'FINISHED'      : FinishedState(),
    'FAILURE'       : FailureState(),
    'TIMEOUT'       : TimeoutState()
}

class JobStateMachine():
    def __init__(self, job_vo):
        self.job_id = job_vo.job_id
        self._state = STATE_DIC[job_vo.state]
        self._created_count = job_vo.created_count
        self._updated_count = job_vo.updated_count

    def inprogress(self):
        if isinstance(self._state, (CreatedState, InprogressState)):
            self._state = InprogressState()
        else:
            raise ERROR_JOB_STATE_CHANGE(action='inprogress', job_id=self.job_id, state=str(self._state))
        return self.get_state()

    def canceled(self):
        if isinstance(self._state, (CreatedState, InprogressState)):
            self._state = CanceledState()
        else:
            raise ERROR_JOB_STATE_CHANGE(action='canceled', job_id=self.job_id, state=str(self._state))
        return self.get_state()

    def finished(self):
        if isinstance(self._state, (InprogressState)):
            self._state = FinishedState()
        else:
            raise ERROR_JOB_STATE_CHANGE(action='finished', job_id=self.job_id, state=str(self._state))
        return self.get_state()

    def timeout(self):
        if isinstance(self._state, (CreatedState, InprogressState)):
            self._state = TimeoutState()
        else:
            raise ERROR_JOB_STATE_CHANGE(action='timeout', job_id=self.job_id, state=str(self._state))
        return self.get_state()

    def failure(self):
        self._state = FailureState()
        return self.get_state()

    def get_state(self):
        return str(self._state)
