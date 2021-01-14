import functools
from spaceone.api.core.v1 import tag_pb2
from spaceone.api.inventory.v1 import server_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.server_model import Server, NIC, Disk
from spaceone.inventory.info.collection_info import CollectionInfo

__all__ = ['ServerInfo', 'ServersInfo']


def ServerNIC(nic_vo: NIC):
    info = {
        'device_index': nic_vo.device_index,
        'device': nic_vo.device,
        'nic_type': nic_vo.nic_type,
        'ip_addresses': nic_vo.ip_addresses,
        'cidr': nic_vo.cidr,
        'mac_address': nic_vo.mac_address,
        'public_ip_address': nic_vo.public_ip_address,
        'tags': change_struct_type(nic_vo.tags) if nic_vo.tags else None
    }
    return server_pb2.ServerNIC(**info)


def ServerDisk(disk_vo: Disk):
    info = {
        'device_index': disk_vo.device_index,
        'device': disk_vo.device,
        'disk_type': disk_vo.disk_type,
        'size': disk_vo.size,
        'tags': change_struct_type(disk_vo.tags) if disk_vo.tags else None
    }
    return server_pb2.ServerDisk(**info)


def ServerInfo(server_vo: Server, minimal=False):
    info = {
        'server_id': server_vo.server_id,
        'name': server_vo.name,
        'state': server_vo.state,
        'primary_ip_address': server_vo.primary_ip_address,
        'server_type': server_vo.server_type,
        'os_type': server_vo.os_type,
        'provider': server_vo.provider,
        'cloud_service_group': server_vo.cloud_service_group,
        'cloud_service_type': server_vo.cloud_service_type,
        'reference': server_pb2.ServerReference(
            **server_vo.reference.to_dict()) if server_vo.reference else None,
        'project_id': server_vo.project_id,
        'region_code': server_vo.region_code,
    }

    if not minimal:
        info.update({
            'ip_addresses': change_list_value_type(server_vo.ip_addresses),
            'data': change_struct_type(server_vo.data),
            'metadata': change_struct_type(server_vo.metadata),
            'nics': list(map(ServerNIC, server_vo.nics)),
            'disks': list(map(ServerDisk, server_vo.disks)),
            'tags': [tag_pb2.Tag(key=tag.key, value=tag.value) for tag in server_vo.tags],
            'collection_info': CollectionInfo(server_vo.collection_info.to_dict()),
            'domain_id': server_vo.domain_id,
            'created_at': change_timestamp_type(server_vo.created_at),
            'updated_at': change_timestamp_type(server_vo.updated_at),
            'deleted_at': change_timestamp_type(server_vo.deleted_at)
        })

    return server_pb2.ServerInfo(**info)


def ServersInfo(server_vos, total_count, **kwargs):
    return server_pb2.ServersInfo(results=list(map(functools.partial(ServerInfo, **kwargs), server_vos)),
                                  total_count=total_count)
