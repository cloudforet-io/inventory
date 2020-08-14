import logging

from spaceone.core.pygrpc.message_type import *

__all__ = ['CollectionInfo']

_LOGGER = logging.getLogger(__name__)


def ChangeHistoryInfo(change_history):
    return {
        'key': change_history.get('key'),
        'job_id': change_history.get('job_id'),
        'diff': change_history.get('diff'),
        'updated_by': change_history.get('updated_by'),
        'updated_at': f"{change_history['updated_at'].isoformat()}Z" if change_history.get('updated_at') else None
    }


def CollectionInfo(data):
    info = {
        'state': data.get('state'),
        'collectors': data.get('collectors', []),
        'service_accounts': data.get('service_accounts', []),
        'secrets': data.get('secrets', []),
        'change_history': list(map(ChangeHistoryInfo, data.get('change_history', []))),
        'pinned_keys': data.get('pinned_keys', [])
    }
    return change_struct_type(info)
