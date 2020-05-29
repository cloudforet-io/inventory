import functools

from spaceone.api.inventory.v1 import network_policy_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.network_policy_model import NetworkPolicy, RoutingTable
from spaceone.inventory.info.zone_info import ZoneInfo
from spaceone.inventory.info.region_info import RegionInfo


__all__ = ['NetworkPolicyInfo', 'NetworkPoliciesInfo']


def RoutingTableInfo(routing_table: RoutingTable):
    info = {
        'cidr': routing_table.cidr,
        'destination': routing_table.destination,
        'interface': routing_table.interface
    }

    return network_policy_pb2.RoutingTable(**info)


def NetworkPolicyInfo(npolicy_vo: NetworkPolicy, minimal=False):
    info = {
        'network_policy_id': npolicy_vo.network_policy_id,
        'name': npolicy_vo.name,
        'reference': network_policy_pb2.NetworkPolicyReference(
            **npolicy_vo.reference.to_dict()) if npolicy_vo.reference else None
    }

    if not minimal:
        info.update({
            'routing_tables': list(map(lambda rt: RoutingTableInfo(rt), npolicy_vo.routing_tables)),
            'dns': change_list_value_type(npolicy_vo.dns),
            'zone_info': ZoneInfo(npolicy_vo.zone),
            'region_info': RegionInfo(npolicy_vo.region),
            'created_at': change_timestamp_type(npolicy_vo.created_at),
            'data': change_struct_type(npolicy_vo.data),
            'metadata': change_struct_type(npolicy_vo.metadata),
            'tags': change_struct_type(npolicy_vo.tags),
            'collection_info': change_struct_type(npolicy_vo.collection_info.to_dict()),
            'domain_id': npolicy_vo.domain_id
        })

    return network_policy_pb2.NetworkPolicyInfo(**info)


def NetworkPoliciesInfo(npolicy_vos, total_count, **kwargs):
    return network_policy_pb2.NetworkPoliciesInfo(results=list(map(functools.partial(NetworkPolicyInfo, **kwargs),
                                                                   npolicy_vos)),
                                                  total_count=total_count)
