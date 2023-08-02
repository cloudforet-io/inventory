import abc
from spaceone.inventory.error import *

# CREATED = 'CREATED'
INPROGRESS = 'IN_PROGRESS'
CANCELED = 'CANCELED'
SUCCESS = 'SUCCESS'
# ERROR = 'ERROR'
FAILURE = 'FAILURE'
# TIMEOUT = 'TIMEOUT'


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

# DEPRECATED
# class CreatedState(JobState):
#     def handle(self):
#         pass
#
#     def __str__(self):
#         return CREATED


class CanceledState(JobState):
    def handle(self):
        pass

    def __str__(self):
        return CANCELED


class SuccessState(JobState):
    def handle(self):
        pass

    def __str__(self):
        return SUCCESS

class FailureState(JobState):
    def handle(self):
        pass

    def __str__(self):
        return FAILURE

# DEPRECATED
# class ErrorState(JobState):
#     def handle(self):
#         pass
#
#     def __str__(self):
#         return ERROR


# DEPRECATED
# class TimeoutState(JobState):
#     def handle(self):
#         pass
#
#     def __str__(self):
#         return TIMEOUT


STATE_DIC = {
    # 'CREATED': CreatedState(),
    'IN_PROGRESS': InprogressState(),
    'CANCELED': CanceledState(),
    'SUCCESS': SuccessState(),
    'FAILURE': SuccessState(),
    # 'ERROR': ErrorState(),
    # 'TIMEOUT': TimeoutState()
}


class JobStateMachine():
    def __init__(self, job_vo):
        self.job_id = job_vo.job_id
        self._status = STATE_DIC[job_vo.status]

    def inprogress(self):
        if isinstance(self._status, (InprogressState, SuccessState)):
            self._status = InprogressState()
        elif isinstance(self._status, (FailureState,)):
            pass
        else:
            raise ERROR_JOB_STATE_CHANGE(action='INPROGRESS', job_id=self.job_id, status=str(self._status))
        return self.get_status()

    def canceled(self):
        if isinstance(self._status, InprogressState):
            self._status = CanceledState()
        else:
            raise ERROR_JOB_STATE_CHANGE(action='CANCELED', job_id=self.job_id, status=str(self._status))
        return self.get_status()

    def success(self):
        if isinstance(self._status, (InprogressState, SuccessState)):
            self._status = SuccessState()
        elif isinstance(self._status, (FailureState,)):
            pass
        else:
            raise ERROR_JOB_STATE_CHANGE(action='SUCCESS', job_id=self.job_id, status=str(self._status))
        return self.get_status()

    def failure(self):
        self._status = FailureState()
        return self.get_status()

    def get_status(self):
        return str(self._status)

    # DEPRECATED
    # def timeout(self):
    #     if isinstance(self._status, (CreatedState, InprogressState)):
    #         self._status = TimeoutState()
    #     else:
    #         raise ERROR_JOB_STATE_CHANGE(action='TIMEOUT', job_id=self.job_id, status=str(self._status))
    #     return self.get_status()

    # DEPRECATED
    def error(self):
        self._status = ErrorState()
        return self.get_status()