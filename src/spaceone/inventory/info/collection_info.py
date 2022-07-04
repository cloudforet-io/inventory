import logging

from spaceone.core.pygrpc.message_type import *

__all__ = ['CollectionInfo']

_LOGGER = logging.getLogger(__name__)


def CollectionInfo(data):
    info = {
        'collectors': data.get('collectors', []),
        'service_accounts': data.get('service_accounts', []),
        'secrets': data.get('secrets', []),
    }
    return change_struct_type(info)
