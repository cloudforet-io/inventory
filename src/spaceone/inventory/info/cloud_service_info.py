import functools
from typing import List
from spaceone.api.inventory.v1 import cloud_service_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_model import CloudService, Tag
from spaceone.inventory.info.collection_info import CollectionInfo

__all__ = ['CloudServiceInfo', 'CloudServicesInfo']


def CloudServiceInfo(cloud_svc_vo: CloudService, minimal=False):
    info = {
        'cloud_service_id': cloud_svc_vo.cloud_service_id,
        'name': cloud_svc_vo.name,
        'state': cloud_svc_vo.state,
        'cloud_service_group': cloud_svc_vo.cloud_service_group,
        'cloud_service_type': cloud_svc_vo.cloud_service_type,
        'provider': cloud_svc_vo.provider,
        'region_code': cloud_svc_vo.region_code,
        'reference': cloud_service_pb2.CloudServiceReference(
            **cloud_svc_vo.reference.to_dict()) if cloud_svc_vo.reference else None,
        'project_id': cloud_svc_vo.project_id,
    }

    if not minimal:
        info.update({
            'account': cloud_svc_vo.account,
            'instance_type': cloud_svc_vo.instance_type,
            'instance_size': cloud_svc_vo.instance_size,
            'ip_addresses': cloud_svc_vo.ip_addresses,
            'data': change_struct_type(cloud_svc_vo.data),
            'metadata': change_struct_type(cloud_svc_vo.metadata),
            'tags': change_struct_type(_tags_to_dict(cloud_svc_vo.tags)),
            'collection_info': CollectionInfo(cloud_svc_vo.collection_info.to_dict()),
            'domain_id': cloud_svc_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(cloud_svc_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(cloud_svc_vo.updated_at),
            'deleted_at': utils.datetime_to_iso8601(cloud_svc_vo.deleted_at),
        })
    return cloud_service_pb2.CloudServiceInfo(**info)


def CloudServicesInfo(cloud_svc_vos, total_count, **kwargs):
    return cloud_service_pb2.CloudServicesInfo(results=list(
        map(functools.partial(CloudServiceInfo, **kwargs), cloud_svc_vos)), total_count=total_count)


def _tags_to_dict(tags: list) -> dict:
    dict_value = {}
    for tag in tags:
        dict_value[tag['key']] = tag['value']
    return dict_value
