import logging

from spaceone.core.pygrpc.message_type import *
from spaceone.api.inventory.v1 import cloud_service_pb2
from spaceone.core import utils

__all__ = ['CollectionInfo']

_LOGGER = logging.getLogger(__name__)


def CollectionInfo(vos: list):
    if vos:
        collections = []
        for vo in vos:
            info = {
                'provider': vo.provider,
                'service_account_id': vo.service_account_id,
                'secret_id': vo.secret_id,
                'collector_id': vo.collector_id,
                'last_collected_at': utils.datetime_to_iso8601(vo.last_collected_at)
            }
            collection = cloud_service_pb2.CollectionInfo(**info)
            collections.append(collection)
        return collections
    else:
        return None
