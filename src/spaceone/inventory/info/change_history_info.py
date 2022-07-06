import functools
from spaceone.api.inventory.v1 import change_history_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.record_model import Record, RecordDiff

__all__ = ['RecordInfo', 'ChangeHistoryInfo']


def RecordDiffInfo(diff_vo: RecordDiff):
    # info = {
    #     'key': diff_vo.key,
    #     'before': change_value_type(diff_vo.before),
    #     'after': change_value_type(diff_vo.after),
    #     'type': diff_vo.type,
    # }
    #
    # return change_history_pb2.RecordDiff(**info)
    return {
        'key': diff_vo.key,
        'before': diff_vo.before,
        'after': diff_vo.after,
        'type': diff_vo.type
    }


def RecordInfo(record_vo: Record, minimal=False):
    info = {
        'record_id': record_vo.record_id,
        'cloud_service_id': record_vo.cloud_service_id,
        'action': record_vo.action,
        'diff_count': record_vo.diff_count,
        'user_id': record_vo.user_id,
        'collector_id': record_vo.collector_id,
        'job_id': record_vo.job_id,
        'created_at': utils.datetime_to_iso8601(record_vo.created_at),
    }

    if not minimal:
        info.update({
            # 'diff': list(map(RecordDiffInfo, record_vo.diff)),
            'diff': change_list_value_type(list(map(RecordDiffInfo, record_vo.diff))),
            'domain_id': record_vo.domain_id
        })

    return change_history_pb2.RecordInfo(**info)


def ChangeHistoryInfo(vos, total_count, **kwargs):
    return change_history_pb2.ChangeHistoryInfo(results=list(map(functools.partial(RecordInfo, **kwargs), vos)),
                                                total_count=total_count)
