import functools

from spaceone.api.inventory.v1 import subnet_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.subnet_model import Subnet
from spaceone.inventory.info.zone_info import ZoneInfo
from spaceone.inventory.info.region_info import RegionInfo
from spaceone.inventory.info.network_info import NetworkInfo
from spaceone.inventory.info.network_type_info import NetworkTypeInfo
from spaceone.inventory.info.network_policy_info import NetworkPolicyInfo

__all__ = ['SubnetInfo', 'SubnetsInfo']


def IPRangeInfo(ip_range):
    info = {
        'start': ip_range.start,
        'end': ip_range.end
    }

    return subnet_pb2.IPRange(**info)


def SubnetInfo(subnet_vo: Subnet, minimal=False):
    info = {
        'subnet_id': subnet_vo.subnet_id,
        'name': subnet_vo.name,
        'reference': subnet_pb2.SubnetReference(
            **subnet_vo.reference.to_dict()) if subnet_vo.reference else None
    }

    if not minimal:
        info.update({
            'cidr': subnet_vo.cidr,
            'gateway': subnet_vo.gateway,
            'ip_ranges': list(map(lambda ip_range: IPRangeInfo(ip_range), subnet_vo.ip_ranges)),
            'vlan': subnet_vo.vlan,
            'network_info': NetworkInfo(subnet_vo.network, minimal=True),
            'network_type_info': {},
            'network_policy_info': {},
            'project_id': subnet_vo.project_id,
            'zone_info': ZoneInfo(subnet_vo.zone, minimal=True),
            'region_info': RegionInfo(subnet_vo.region, minimal=True),
            'created_at': change_timestamp_type(subnet_vo.created_at),
            'data': change_struct_type(subnet_vo.data),
            'metadata': change_struct_type(subnet_vo.metadata),
            'tags': change_struct_type(subnet_vo.tags),
            'collection_info': change_struct_type(subnet_vo.collection_info.to_dict()),
            'domain_id': subnet_vo.domain_id
        })

        if subnet_vo.network_type:
            info.update({
                'network_type_info': NetworkTypeInfo(subnet_vo.network_type, minimal=True),
            })

        if subnet_vo.network_policy:
            info.update({
                'network_policy_info': NetworkPolicyInfo(subnet_vo.network_policy, minimal=True),
            })

    return subnet_pb2.SubnetInfo(**info)


def SubnetsInfo(subnet_vos, total_count, **kwargs):
    return subnet_pb2.SubnetsInfo(results=list(map(functools.partial(SubnetInfo, **kwargs), subnet_vos)), total_count=total_count)
