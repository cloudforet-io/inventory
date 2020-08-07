import functools

from spaceone.api.inventory.v1 import task_item_pb2, collector_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.task_item_model import JobTask

__all__ = ['TaskItemInfo', 'TaskItemsInfo']

def ErrorInfo(error):
    info = {
        'error_code': error.error_code,
        'message': error.message,
        'additional': change_struct_type(error.additional)
    }
    return collector_pb2.ErrorInfo(**info)

def TaskItemInfo(task_item_vo: JobTask, minimal=False):
    info = {
        'resource_id': task_item_vo.resource_id,
        'resource_type': task_item_vo.resource_type,
        'job_task_id': task_item_vo.job_task_id,
        'job_id': task_item_vo.job_id,
        'state': task_item_vo.state,
        'project_id': task_item_vo.project_id,
        'domain_id': task_item_vo.domain_id,
    }

    if not minimal:
        info.update({
            'references': change_list_value_type(task_item_vo.references)
        })
        if task_item_vo.error:
            info.update({
                'error': ErrorInfo(task_item_vo.error)
            })

    return task_item_pb2.TaskItemInfo(**info)


def TaskItemsInfo(vos, total_count, **kwargs):
    return task_item_pb2.TaskItemsInfo(results=list(map(functools.partial(TaskItemInfo, **kwargs), vos)), total_count=total_count) 
