import functools
from spaceone.api.inventory.v1 import cloud_service_query_set_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_query_set_model import CloudServiceQuerySet

__all__ = ['CloudServiceQuerySetInfo', 'CloudServiceQuerySetsInfo']


def CloudServiceQuerySetInfo(cloud_svc_query_set_vo: CloudServiceQuerySet, minimal=False):
    info = {
        'query_set_id': cloud_svc_query_set_vo.query_set_id,
        'name': cloud_svc_query_set_vo.name,
        'state': cloud_svc_query_set_vo.state,
        'query_type': cloud_svc_query_set_vo.query_type,
        'provider': cloud_svc_query_set_vo.provider,
        'cloud_service_group': cloud_svc_query_set_vo.cloud_service_group,
        'cloud_service_type': cloud_svc_query_set_vo.cloud_service_type,
    }

    if not minimal:
        info.update({
            'query_options': change_analyze_query(cloud_svc_query_set_vo.query_options),
            'keys': cloud_svc_query_set_vo.keys,
            'additional_info_keys': cloud_svc_query_set_vo.additional_info_keys,
            'unit': change_struct_type(cloud_svc_query_set_vo.unit),
            'tags': change_struct_type(cloud_svc_query_set_vo.tags),
            'domain_id': cloud_svc_query_set_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(cloud_svc_query_set_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(cloud_svc_query_set_vo.updated_at)
        })

    return cloud_service_query_set_pb2.CloudServiceQuerySetInfo(**info)


def CloudServiceQuerySetsInfo(cloud_svc_query_set_vos, total_count, **kwargs):
    return cloud_service_query_set_pb2.CloudServiceQuerySetsInfo(
        results=list(map(functools.partial(CloudServiceQuerySetInfo, **kwargs), cloud_svc_query_set_vos)),
        total_count=total_count)
