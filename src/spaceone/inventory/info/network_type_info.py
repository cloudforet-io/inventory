import functools

from spaceone.api.inventory.v1 import network_type_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.network_type_model import NetworkType

__all__ = ['NetworkTypeInfo', 'NetworkTypesInfo']


def NetworkTypeInfo(ntype_vo: NetworkType, minimal=False):
    info = {
        'network_type_id': ntype_vo.network_type_id,
        'name': ntype_vo.name
    }

    if not minimal:
        info.update({
            'created_at': change_timestamp_type(ntype_vo.created_at),
            'tags': change_struct_type(ntype_vo.tags),
            'domain_id': ntype_vo.domain_id
        })

    return network_type_pb2.NetworkTypeInfo(**info)


def NetworkTypesInfo(ntype_vos, total_count, **kwargs):
    return network_type_pb2.NetworkTypesInfo(results=list(map(functools.partial(NetworkTypeInfo, **kwargs), ntype_vos)),
                                             total_count=total_count)
