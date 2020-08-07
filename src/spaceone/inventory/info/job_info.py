import functools

from spaceone.api.inventory.v1 import collector_pb2, job_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.job_model import Job
from spaceone.inventory.info.collector_info import CollectorInfo

__all__ = ['JobInfo', 'JobsInfo']

def ErrorInfo(error):
    info = {
        'error_code': error.error_code,
        'message': error.message,
        'additional': change_struct_type(error.additional)
    }
    return collector_pb2.ErrorInfo(**info)

def JobInfo(job_vo: Job, minimal=False):
    info = {
        'job_id': job_vo.job_id,
        'total_tasks': job_vo.total_tasks,
        'created_at': change_timestamp_type(job_vo.created_at),
        'finished_at': change_timestamp_type(job_vo.finished_at),
        'state': job_vo.state,
        'collect_mode': job_vo.collect_mode
    }

    if not minimal:
        info.update({
            'collector_info': CollectorInfo(job_vo.collector, minimal=True) if job_vo.collector else None,
            'created_count': job_vo.created_count,
            'updated_count': job_vo.updated_count,
            'filter': change_struct_type(job_vo.filters),
            'errors': list(map(functools.partial(ErrorInfo), job_vo.errors))
        })

    return collector_pb2.JobInfo(**info)


def JobsInfo(vos, total_count, **kwargs):
    return job_pb2.JobsInfo(results=list(map(functools.partial(JobInfo, **kwargs), vos)), total_count=total_count) 
