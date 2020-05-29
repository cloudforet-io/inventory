import functools

from spaceone.api.inventory.v1 import network_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.network_model import Network
from spaceone.inventory.info.zone_info import ZoneInfo
from spaceone.inventory.info.region_info import RegionInfo

__all__ = ['NetworkInfo', 'NetworksInfo']


def NetworkInfo(network_vo: Network, minimal=False):
    info = {
        'network_id': network_vo.network_id,
        'name': network_vo.name,
        'reference': network_pb2.NetworkReference(
            **network_vo.reference.to_dict()) if network_vo.reference else None
    }

    if not minimal:
        info.update({
            'cidr': network_vo.cidr,
            'zone_info': ZoneInfo(network_vo.zone, minimal=True),
            'region_info': RegionInfo(network_vo.region, minimal=True),
            'created_at': change_timestamp_type(network_vo.created_at),
            'data': change_struct_type(network_vo.data),
            'metadata': change_struct_type(network_vo.metadata),
            'tags': change_struct_type(network_vo.tags),
            'collection_info': change_struct_type(network_vo.collection_info.to_dict()),
            'domain_id': network_vo.domain_id
        })

    return network_pb2.NetworkInfo(**info)


def NetworksInfo(network_vos, total_count, **kwargs):
    return network_pb2.NetworksInfo(results=list(map(functools.partial(NetworkInfo, **kwargs), network_vos)), total_count=total_count)
