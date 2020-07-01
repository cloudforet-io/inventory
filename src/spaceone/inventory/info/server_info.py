import functools
from spaceone.api.inventory.v1 import server_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.server_model import Server
from spaceone.inventory.info.region_info import RegionInfo
from spaceone.inventory.info.collection_info import CollectionInfo

__all__ = ['ServerInfo', 'ServersInfo']


def ServerInfo(server_vo: Server, minimal=False):
    info = {
        'server_id': server_vo.server_id,
        'name': server_vo.name,
        'state': server_vo.state,
        'primary_ip_address': server_vo.primary_ip_address,
        'server_type': server_vo.server_type,
        'os_type': server_vo.os_type,
        'provider': server_vo.provider,
        'reference': server_pb2.ServerReference(
            **server_vo.reference.to_dict()) if server_vo.reference else None,
        'project_id': server_vo.project_id
    }

    if not minimal:
        server_data = server_vo.to_dict()
        info.update({
            'ip_addresses': change_list_value_type(server_vo.ip_addresses),
            'data': change_struct_type(server_vo.data),
            'metadata': change_struct_type(server_vo.metadata),
            'nics': change_list_value_type(server_data['nics']),
            'disks': change_list_value_type(server_data['disks']),
            'tags': change_struct_type(server_vo.tags),
            'collection_info': CollectionInfo(server_vo.collection_info.to_dict()),
            'region_info': RegionInfo(server_vo.region, minimal=True) if server_vo.region else None,
            'domain_id': server_vo.domain_id,
            'created_at': change_timestamp_type(server_vo.created_at),
            'updated_at': change_timestamp_type(server_vo.updated_at),
            'deleted_at': change_timestamp_type(server_vo.deleted_at)
        })

    return server_pb2.ServerInfo(**info)


def ServersInfo(server_vos, total_count, **kwargs):
    return server_pb2.ServersInfo(results=list(map(functools.partial(ServerInfo, **kwargs), server_vos)),
                                  total_count=total_count)
