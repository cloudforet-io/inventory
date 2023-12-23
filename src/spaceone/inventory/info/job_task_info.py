import functools
from spaceone.api.inventory.v1 import job_task_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.job_task_model import JobTask

__all__ = ["JobTaskInfo", "JobTasksInfo"]


def ErrorInfo(error):
    info = {
        "error_code": error.error_code,
        "message": error.message,
        "additional": change_struct_type(error.additional),
    }
    return job_task_pb2.ErrorInfo(**info)


def JobTaskInfo(job_task_vo: JobTask, minimal=False):
    info = {
        "job_task_id": job_task_vo.job_task_id,
        "status": job_task_vo.status,
        "created_count": job_task_vo.created_count,
        "updated_count": job_task_vo.updated_count,
        "disconnected_count": job_task_vo.disconnected_count,
        "deleted_count": job_task_vo.deleted_count,
        "failure_count": job_task_vo.failure_count,
        "job_id": job_task_vo.job_id,
        "created_at": utils.datetime_to_iso8601(job_task_vo.created_at),
        "started_at": utils.datetime_to_iso8601(job_task_vo.started_at),
        "finished_at": utils.datetime_to_iso8601(job_task_vo.finished_at),
    }

    if not minimal:
        info.update(
            {
                "errors": list(map(functools.partial(ErrorInfo), job_task_vo.errors)),
                "secret_id": job_task_vo.secret_id,
                "provider": job_task_vo.provider,
                "service_account_id": job_task_vo.service_account_id,
                "project_id": job_task_vo.project_id,
                "workspace_id": job_task_vo.workspace_id,
                "domain_id": job_task_vo.domain_id,
            }
        )

    return job_task_pb2.JobTaskInfo(**info)


def JobTasksInfo(vos, total_count, **kwargs):
    return job_task_pb2.JobTasksInfo(
        results=list(map(functools.partial(JobTaskInfo, **kwargs), vos)),
        total_count=total_count,
    )
