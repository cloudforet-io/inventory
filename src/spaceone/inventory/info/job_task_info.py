import functools

from spaceone.api.inventory.v1 import job_task_pb2, collector_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.job_task_model import JobTask

__all__ = ['JobTaskInfo', 'JobTasksInfo']


def ErrorInfo(error):
    info = {
        'error_code': error.error_code,
        'message': error.message,
        'additional': change_struct_type(error.additional)
    }
    return collector_pb2.ErrorInfo(**info)

def JobTaskInfo(job_task_vo: JobTask, minimal=False):
    info = {
        'job_task_id': job_task_vo.job_task_id,
        'job_id': job_task_vo.job.job_id,
        'project_id': job_task_vo.project_id,
        'domain_id': job_task_vo.domain_id,
        'state': job_task_vo.state,
        'started_at': change_timestamp_type(job_task_vo.started_at),
        'finished_at': change_timestamp_type(job_task_vo.finished_at)
    }

    if not minimal:
        info.update({
            'errors': list(map(functools.partial(ErrorInfo), job_task_vo.errors))
        })

    return job_task_pb2.JobTaskInfo(**info)


def JobTasksInfo(vos, total_count, **kwargs):
    return job_task_pb2.JobTasksInfo(results=list(map(functools.partial(JobTaskInfo, **kwargs), vos)), total_count=total_count)
