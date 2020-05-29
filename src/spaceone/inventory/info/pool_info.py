import functools

from spaceone.api.inventory.v1 import pool_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.pool_model import Pool
from spaceone.inventory.info.region_info import RegionInfo
from spaceone.inventory.info.zone_info import ZoneInfo

__all__ = ['PoolInfo', 'PoolsInfo', 'PoolMemberInfo', 'PoolMembersInfo']


def PoolInfo(pool_vo: Pool, minimal=False):
    info = {
        'pool_id': pool_vo.pool_id,
        'name': pool_vo.name,
        'state': pool_vo.state
    }

    if not minimal:
        info.update({
            'created_at': change_timestamp_type(pool_vo.created_at),
            'deleted_at': change_timestamp_type(pool_vo.deleted_at),
            'tags': change_struct_type(pool_vo.tags),
            'domain_id': pool_vo.domain_id,
            'region_info': RegionInfo(pool_vo.region, minimal=True),
            'zone_info': ZoneInfo(pool_vo.zone, minimal=True)
        })

    return pool_pb2.PoolInfo(**info)


def PoolsInfo(pool_vos, total_count, **kwargs):
    return pool_pb2.PoolsInfo(results=list(map(functools.partial(PoolInfo, **kwargs), pool_vos)),
                              total_count=total_count)


def PoolMemberInfo(pool_map_info):
    info = {
        'pool_info': PoolInfo(pool_map_info['pool'], minimal=True),
        'user_info': change_struct_type(pool_map_info['user']),
        'labels': change_list_value_type(pool_map_info['labels'])
    }

    return pool_pb2.PoolMemberInfo(**info)


def PoolMembersInfo(pool_map_vos, total_count):
    results = list(map(lambda pool_map_vo: PoolMemberInfo(pool_map_vo), pool_map_vos))

    return pool_pb2.PoolMembersInfo(results=results, total_count=total_count)