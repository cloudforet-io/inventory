import functools
from spaceone.api.inventory.v1 import cloud_service_type_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType

__all__ = ['CloudServiceTypeInfo', 'CloudServiceTypesInfo']


def CloudServiceTypeInfo(cloud_svc_type_vo: CloudServiceType, minimal=False):
    info = {
        'cloud_service_type_id': cloud_svc_type_vo.cloud_service_type_id,
        'name': cloud_svc_type_vo.name,
        'provider': cloud_svc_type_vo.provider,
        'group': cloud_svc_type_vo.group,
        'service_code': cloud_svc_type_vo.service_code,
        'is_primary': cloud_svc_type_vo.is_primary,
        'is_major': cloud_svc_type_vo.is_major,
        'resource_type': cloud_svc_type_vo.resource_type
    }

    if not minimal:
        info.update({
            'cloud_service_type_key': cloud_svc_type_vo.cloud_service_type_key,
            'metadata': change_struct_type(cloud_svc_type_vo.metadata),
            'labels': change_list_value_type(cloud_svc_type_vo.labels),
            'tags': change_struct_type(cloud_svc_type_vo.tags),
            'domain_id': cloud_svc_type_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(cloud_svc_type_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(cloud_svc_type_vo.updated_at)
        })

    return cloud_service_type_pb2.CloudServiceTypeInfo(**info)


def CloudServiceTypesInfo(cloud_svc_type_vos, total_count, **kwargs):
    return cloud_service_type_pb2.CloudServiceTypesInfo(results=list(map(functools.partial(CloudServiceTypeInfo, **kwargs),
                                                                         cloud_svc_type_vos)),
                                                        total_count=total_count)
