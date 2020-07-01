import functools
from spaceone.api.inventory.v1 import cloud_service_type_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.info.collection_info import CollectionInfo

__all__ = ['CloudServiceTypeInfo', 'CloudServiceTypesInfo']


def CloudServiceTypeInfo(cloud_svc_type_vo: CloudServiceType, minimal=False):
    info = {
        'cloud_service_type_id': cloud_svc_type_vo.cloud_service_type_id,
        'name': cloud_svc_type_vo.name,
        'group': cloud_svc_type_vo.group,
        'provider': cloud_svc_type_vo.provider
    }

    if not minimal:
        info.update({
            'metadata': change_struct_type(cloud_svc_type_vo.metadata),
            'labels': change_list_value_type(cloud_svc_type_vo.labels),
            'domain_id': cloud_svc_type_vo.domain_id,
            'tags': change_struct_type(cloud_svc_type_vo.tags),
            'collection_info': CollectionInfo(cloud_svc_type_vo.collection_info.to_dict()),
            'created_at': change_timestamp_type(cloud_svc_type_vo.created_at),
            'updated_at': change_timestamp_type(cloud_svc_type_vo.updated_at)
        })

    if getattr(cloud_svc_type_vo, 'cloud_service_count', None) is not None:
        info.update({
            'cloud_service_count': cloud_svc_type_vo.cloud_service_count
        })

    return cloud_service_type_pb2.CloudServiceTypeInfo(**info)


def CloudServiceTypesInfo(cloud_svc_type_vos, total_count, **kwargs):
    return cloud_service_type_pb2.CloudServiceTypesInfo(results=list(map(functools.partial(CloudServiceTypeInfo, **kwargs),
                                                                         cloud_svc_type_vos)),
                                                        total_count=total_count)
