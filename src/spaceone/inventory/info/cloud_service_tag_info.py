import functools
from spaceone.core import utils

from spaceone.api.inventory.v1 import cloud_service_tag_pb2

from spaceone.inventory.model.cloud_service_tag_model import CloudServiceTag

__all__ = ['CloudServiceTagInfo', 'CloudServiceTagsInfo']


def CloudServiceTagInfo(cloud_svc_tag_vo: CloudServiceTag, minimal=False):
    info = {
        'cloud_service_id': cloud_svc_tag_vo.cloud_service_id,
        'key': cloud_svc_tag_vo.key,
        'value': cloud_svc_tag_vo.value,
        'type': cloud_svc_tag_vo.type,
        'provider': cloud_svc_tag_vo.provider,
    }

    if not minimal:
        info.update({
            'created_at': utils.datetime_to_iso8601(cloud_svc_tag_vo.created_at),
            'domain_id': cloud_svc_tag_vo.domain_id
        })

    return cloud_service_tag_pb2.CloudServiceTagInfo(**info)


def CloudServiceTagsInfo(cloud_svc_tag_vos, total_count, **kwargs):
    return cloud_service_tag_pb2.CloudServiceTagsInfo(
        results=list(map(functools.partial(CloudServiceTagInfo, **kwargs),
                         cloud_svc_tag_vos)),
        total_count=total_count)
