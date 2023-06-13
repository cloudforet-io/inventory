import abc
from spaceone.inventory.error import *


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
    'PENDING': PendingState(),
    'CANCELED': CanceledState(),
    'IN_PROGRESS': InprogressState(),
    'SUCCESS': SuccessState(),
    'FAILURE': FailureState()
}


class JobTaskStateMachine(object):
    def __init__(self, job_task_vo):
        self.job_task_id = job_task_vo.job_task_id
        self._state = STATE_DIC[job_task_vo.status]

    def inprogress(self):
        if isinstance(self._state, (PendingState, InprogressState, SuccessState, FailureState)):
            self._state = InprogressState()
        else:
            raise ERROR_JOB_TASK_STATE_CHANGE(action='INPROGRESS', job_task_id=self.job_task_id, status=str(self._state))
        return self.get_state()

    def success(self):
        if isinstance(self._state, (InprogressState)):
            self._state = SuccessState()
        else:
            raise ERROR_JOB_TASK_STATE_CHANGE(action='SUCCESS', job_task_id=self.job_task_id, status=str(self._state))
        return self.get_state()

    def failure(self):
        self._state = FailureState()
        return self.get_state()

    def canceled(self):
        self._state = CanceledState()
        return self.get_state()

    def get_state(self):
        return str(self._state)
