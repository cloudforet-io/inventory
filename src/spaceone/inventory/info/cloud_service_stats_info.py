import functools
from spaceone.api.inventory.v1 import cloud_service_stats_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.inventory.model.cloud_service_stats_model import CloudServiceStats

__all__ = ["CloudServiceStatInfo", "CloudServiceStatsInfo"]


def CloudServiceStatInfo(cloud_svc_stat_vo: CloudServiceStats, minimal=False):
    info = {
        "query_set_id": cloud_svc_stat_vo.query_set_id,
        "data": change_struct_type(cloud_svc_stat_vo.data),
        "unit": change_struct_type(cloud_svc_stat_vo.unit),
        "provider": cloud_svc_stat_vo.provider,
        "cloud_service_group": cloud_svc_stat_vo.cloud_service_group,
        "cloud_service_type": cloud_svc_stat_vo.cloud_service_type,
        "project_id": cloud_svc_stat_vo.project_id,
        "created_date": cloud_svc_stat_vo.created_date,
    }

    if not minimal:
        info.update(
            {
                "region_code": cloud_svc_stat_vo.region_code,
                "account": cloud_svc_stat_vo.account,
                "additional_info": change_struct_type(
                    cloud_svc_stat_vo.additional_info
                ),
                "workspace_id": cloud_svc_stat_vo.workspace_id,
                "domain_id": cloud_svc_stat_vo.domain_id,
            }
        )

    return cloud_service_stats_pb2.CloudServiceStatInfo(**info)


def CloudServiceStatsInfo(cloud_svc_stats_vos, total_count, **kwargs):
    return cloud_service_stats_pb2.CloudServiceStatsInfo(
        results=list(
            map(functools.partial(CloudServiceStatInfo, **kwargs), cloud_svc_stats_vos)
        ),
        total_count=total_count,
    )
