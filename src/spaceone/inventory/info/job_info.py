import functools
from spaceone.api.inventory.v1 import collector_pb2, job_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
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
        'status': job_vo.status,
        'created_at': utils.datetime_to_iso8601(job_vo.created_at),
        'finished_at': utils.datetime_to_iso8601(job_vo.finished_at),
    }

    if not minimal:
        info.update({
            'filter': change_struct_type(job_vo.filters),
            'total_tasks': job_vo.total_tasks,
            'remained_tasks': job_vo.remained_tasks,
            'errors': list(map(functools.partial(ErrorInfo), job_vo.errors)),
            'collector_info': CollectorInfo(job_vo.collector, minimal=True) if job_vo.collector else None,
            'project_id': job_vo.project_id,
            'domain_id': job_vo.domain_id,
            'updated_at': utils.datetime_to_iso8601(job_vo.updated_at),
        })

        # Temporary code for DB migration
        if not job_vo.collector_id and job_vo.collector:
            job_vo.update({'collector_id': job_vo.collector.collector_id})

    return collector_pb2.JobInfo(**info)


def JobsInfo(vos, total_count, **kwargs):
    return job_pb2.JobsInfo(results=list(map(functools.partial(JobInfo, **kwargs), vos)), total_count=total_count)

