import functools
from spaceone.api.inventory.v1 import change_history_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.change_history_model import Record

__all__ = ['RecordInfo', 'ChangeHistoryInfo']


def RecordDiff(diff_vo):
    info = {
        'key': diff_vo['key'],
        'before': change_value_type(diff_vo.get('before')),
        'after': change_value_type(diff_vo.get('after')),
    }

    return change_history_pb2.RecordDiff(**info)


def RecordInfo(record_vo: Record, minimal=False):
    info = {
        'record_id': record_vo.record_id,
        'cloud_service_id': record_vo.cloud_service_id,
        'action': record_vo.action,
        'user_id': record_vo.user_id,
        'collector_id': record_vo.collector_id,
        'job_id': record_vo.job_id,
        'created_at': utils.datetime_to_iso8601(record_vo.created_at),
    }

    if not minimal:
        info.update({
            'diff': list(map(RecordDiff, record_vo.diff)),
            'domain_id': record_vo.domain_id
        })

    return change_history_pb2.RecordInfo(**info)


def ChangeHistoryInfo(vos, total_count, **kwargs):
    return change_history_pb2.ChangeHistoryInfo(results=list(map(functools.partial(RecordInfo, **kwargs), vos)),
                                                total_count=total_count)
