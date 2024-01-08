import functools
from spaceone.api.inventory.v1 import job_pb2
from spaceone.core import utils
from spaceone.inventory.model.job_model import Job

__all__ = ["JobInfo", "JobsInfo"]


def JobInfo(job_vo: Job, minimal=False):
    info = {
        "job_id": job_vo.job_id,
        "status": job_vo.status,
        "collector_id": job_vo.collector_id,
        "plugin_id": job_vo.plugin_id,
        "created_at": utils.datetime_to_iso8601(job_vo.created_at),
        "finished_at": utils.datetime_to_iso8601(job_vo.finished_at),
    }

    if not minimal:
        info.update(
            {
                "total_tasks": job_vo.total_tasks,
                "remained_tasks": job_vo.remained_tasks,
                "success_tasks": job_vo.success_tasks,
                "failure_tasks": job_vo.failure_tasks,
                "request_secret_id": job_vo.request_secret_id,
                "request_workspace_id": job_vo.request_workspace_id,
                "resource_group": job_vo.resource_group,
                "workspace_id": job_vo.workspace_id,
                "domain_id": job_vo.domain_id,
                "updated_at": utils.datetime_to_iso8601(job_vo.updated_at),
            }
        )

    return job_pb2.JobInfo(**info)


def JobsInfo(vos, total_count, **kwargs):
    return job_pb2.JobsInfo(
        results=list(map(functools.partial(JobInfo, **kwargs), vos)),
        total_count=total_count,
    )
