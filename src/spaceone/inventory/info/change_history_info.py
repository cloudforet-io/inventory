import functools
from spaceone.api.inventory.v1 import change_history_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.record_model import Record, RecordDiff

__all__ = ["RecordInfo", "ChangeHistoryInfo"]


def RecordDiffInfo(diff_vo: RecordDiff):
    return {
        "key": diff_vo.key,
        "before": diff_vo.before,
        "after": diff_vo.after,
        "type": diff_vo.type,
    }


def RecordInfo(record_vo: Record, minimal=False):
    info = {
        "record_id": record_vo.record_id,
        "action": record_vo.action,
        "diff_count": record_vo.diff_count,
        "cloud_service_id": record_vo.cloud_service_id,
        "updated_by": record_vo.updated_by,
        "user_id": record_vo.user_id,
        "collector_id": record_vo.collector_id,
        "job_id": record_vo.job_id,
    }

    if not minimal:
        info.update(
            {
                "diff": change_list_value_type(
                    list(map(RecordDiffInfo, record_vo.diff))
                ),
                "domain_id": record_vo.domain_id,
                "created_at": utils.datetime_to_iso8601(record_vo.created_at),
            }
        )

    return change_history_pb2.RecordInfo(**info)


def ChangeHistoryInfo(vos, total_count, **kwargs):
    return change_history_pb2.ChangeHistoryInfo(
        results=list(map(functools.partial(RecordInfo, **kwargs), vos)),
        total_count=total_count,
    )
