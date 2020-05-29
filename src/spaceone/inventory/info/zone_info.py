import functools

from spaceone.api.inventory.v1 import zone_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.zone_model import Zone
from spaceone.inventory.info.region_info import RegionInfo

__all__ = ['ZoneInfo', 'ZonesInfo', 'ZoneMemberInfo', 'ZoneMembersInfo']


def ZoneInfo(zone_vo: Zone, minimal=False):
    info = {
        'zone_id': zone_vo.zone_id,
        'name': zone_vo.name,
        'state': zone_vo.state
    }

    if not minimal:
        info.update({
            'created_at': change_timestamp_type(zone_vo.created_at),
            'deleted_at': change_timestamp_type(zone_vo.deleted_at),
            'tags': change_struct_type(zone_vo.tags),
            'domain_id': zone_vo.domain_id,
            'region_info': RegionInfo(zone_vo.region, minimal=True)
        })

    return zone_pb2.ZoneInfo(**info)


def ZonesInfo(zone_vos, total_count, **kwargs):
    return zone_pb2.ZonesInfo(results=list(map(functools.partial(ZoneInfo, **kwargs), zone_vos)),
                              total_count=total_count)


def ZoneMemberInfo(zone_map_info):
    info = {
        'zone_info': ZoneInfo(zone_map_info['zone'], minimal=True),
        'user_info': change_struct_type(zone_map_info['user']),
        'labels': change_list_value_type(zone_map_info['labels'])
    }

    return zone_pb2.ZoneMemberInfo(**info)


def ZoneMembersInfo(zone_map_vos, total_count, **kwargs):
    results = list(map(lambda zone_map_vo: ZoneMemberInfo(zone_map_vo), zone_map_vos))

    return zone_pb2.ZoneMembersInfo(results=results, total_count=total_count)
