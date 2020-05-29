import functools
from spaceone.api.inventory.v1 import cloud_service_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.info.region_info import RegionInfo

__all__ = ['CloudServiceInfo', 'CloudServicesInfo']


def CloudServiceInfo(cloud_svc_vo: CloudService, minimal=False):
    info = {
        'cloud_service_id': cloud_svc_vo.cloud_service_id,
        'cloud_service_type': cloud_svc_vo.cloud_service_type,
        'cloud_service_group': cloud_svc_vo.cloud_service_group,
        'provider': cloud_svc_vo.provider,
        'reference': cloud_service_pb2.CloudServiceReference(
            **cloud_svc_vo.reference.to_dict()) if cloud_svc_vo.reference else None
    }

    if not minimal:
        info.update({
            'data': change_struct_type(cloud_svc_vo.data),
            'metadata': change_struct_type(cloud_svc_vo.metadata),
            'region_info': RegionInfo(cloud_svc_vo.region, minimal=True) if cloud_svc_vo.region else None,
            'project_id': cloud_svc_vo.project_id,
            'domain_id': cloud_svc_vo.domain_id,
            'tags': change_struct_type(cloud_svc_vo.tags),
            'collection_info': change_struct_type(cloud_svc_vo.collection_info.to_dict()),
            'created_at': change_timestamp_type(cloud_svc_vo.created_at),
            'updated_at': change_timestamp_type(cloud_svc_vo.updated_at)
        })

    return cloud_service_pb2.CloudServiceInfo(**info)


def CloudServicesInfo(cloud_svc_vos, total_count, **kwargs):
    return cloud_service_pb2.CloudServicesInfo(results=list(
        map(functools.partial(CloudServiceInfo, **kwargs), cloud_svc_vos)), total_count=total_count)
