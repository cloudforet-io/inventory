import functools
import logging
from spaceone.api.inventory.v1 import region_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.region_model import Region

__all__ = ['RegionInfo', 'RegionsInfo', 'RegionMemberInfo', 'RegionMembersInfo']

_LOGGER = logging.getLogger()


def RegionInfo(region_vo: Region, minimal=False):
    info = {
        'region_id': region_vo.region_id,
        'name': region_vo.name,
        'state': region_vo.state
    }

    if not minimal:
        info.update({
            'created_at': change_timestamp_type(region_vo.created_at),
            'deleted_at': change_timestamp_type(region_vo.deleted_at),
            'tags': change_struct_type(region_vo.tags),
            'domain_id': region_vo.domain_id
        })

    return region_pb2.RegionInfo(**info)


def RegionsInfo(region_vos, total_count, **kwargs):
    return region_pb2.RegionsInfo(results=list(map(functools.partial(RegionInfo, **kwargs), region_vos)),
                                  total_count=total_count)


def RegionMemberInfo(region_map_info):
    info = {
        'region_info': RegionInfo(region_map_info['region'], minimal=True),
        'user_info': change_struct_type(region_map_info['user']),
        'labels': change_list_value_type(region_map_info['labels'])
    }

    return region_pb2.RegionMemberInfo(**info)


def RegionMembersInfo(region_map_vos, total_count, **kwargs):
    results = list(map(lambda region_map_vo: RegionMemberInfo(region_map_vo), region_map_vos))

    return region_pb2.RegionMembersInfo(results=results, total_count=total_count)
