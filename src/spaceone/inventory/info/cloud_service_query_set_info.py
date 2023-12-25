import functools
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_query_set_model import CloudServiceQuerySet

__all__ = ["CloudServiceQuerySetInfo", "CloudServiceQuerySetsInfo"]


def CloudServiceQuerySetInfo(
    cloud_svc_query_set_vo: CloudServiceQuerySet, minimal=False
):
    info = {
        "query_set_id": cloud_svc_query_set_vo.query_set_id,
        "name": cloud_svc_query_set_vo.name,
        "state": cloud_svc_query_set_vo.state,
        "query_type": cloud_svc_query_set_vo.query_type,
        "provider": cloud_svc_query_set_vo.provider,
        "cloud_service_group": cloud_svc_query_set_vo.cloud_service_group,
        "cloud_service_type": cloud_svc_query_set_vo.cloud_service_type,
    }

    if not minimal:
        info.update(
            {
                "query_options": cloud_svc_query_set_vo.query_options,
                "additional_info_keys": cloud_svc_query_set_vo.additional_info_keys,
                "data_keys": cloud_svc_query_set_vo.data_keys,
                "unit": cloud_svc_query_set_vo.unit,
                "tags": cloud_svc_query_set_vo.tags,
                "resource_group": cloud_svc_query_set_vo.resource_group,
                "workspace_id": cloud_svc_query_set_vo.workspace_id,
                "domain_id": cloud_svc_query_set_vo.domain_id,
                "created_at": utils.datetime_to_iso8601(
                    cloud_svc_query_set_vo.created_at
                ),
                "updated_at": utils.datetime_to_iso8601(
                    cloud_svc_query_set_vo.updated_at
                ),
            }
        )

    return info


def CloudServiceQuerySetsInfo(cloud_svc_query_set_vos, total_count, **kwargs):
    return {
        "results": list(
            map(
                functools.partial(CloudServiceQuerySetInfo, **kwargs),
                cloud_svc_query_set_vos,
            )
        ),
        "total_count": total_count,
    }
