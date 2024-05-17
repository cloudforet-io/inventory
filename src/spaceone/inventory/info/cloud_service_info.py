import functools
from spaceone.api.inventory.v1 import cloud_service_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_model import CloudService

__all__ = ["CloudServiceInfo", "CloudServicesInfo"]


def CollectionInfo(vo):
    if vo:
        info = {
            "service_account_id": vo.service_account_id,
            "secret_id": vo.secret_id,
            "collector_id": vo.collector_id,
            "last_collected_at": utils.datetime_to_iso8601(vo.last_collected_at),
        }
        return cloud_service_pb2.CollectionInfo(**info)
    else:
        return None


def CloudServiceInfo(cloud_svc_vo: CloudService, minimal=False, include_metadata=True):
    info = {
        "cloud_service_id": cloud_svc_vo.cloud_service_id,
        "name": cloud_svc_vo.name,
        "state": cloud_svc_vo.state,
        "cloud_service_group": cloud_svc_vo.cloud_service_group,
        "cloud_service_type": cloud_svc_vo.cloud_service_type,
        "provider": cloud_svc_vo.provider,
        "region_code": cloud_svc_vo.region_code,
        "reference": cloud_service_pb2.CloudServiceReference(
            **cloud_svc_vo.reference.to_dict()
        )
        if cloud_svc_vo.reference
        else None,
        "project_id": cloud_svc_vo.project_id,
    }

    if not minimal:
        info.update(
            {
                "account": cloud_svc_vo.account,
                "instance_type": cloud_svc_vo.instance_type,
                "instance_size": cloud_svc_vo.instance_size,
                "ip_addresses": cloud_svc_vo.ip_addresses,
                "data": change_struct_type(cloud_svc_vo.data),
                "tags": change_struct_type(
                    _change_tags_without_hash(cloud_svc_vo.tags)
                ),
                "tag_keys": change_struct_type(cloud_svc_vo.tag_keys),
                "collection_info": CollectionInfo(cloud_svc_vo.collection_info),
                "workspace_id": cloud_svc_vo.workspace_id,
                "domain_id": cloud_svc_vo.domain_id,
                "created_at": utils.datetime_to_iso8601(cloud_svc_vo.created_at),
                "updated_at": utils.datetime_to_iso8601(cloud_svc_vo.updated_at),
                "deleted_at": utils.datetime_to_iso8601(cloud_svc_vo.deleted_at),
            }
        )

        if include_metadata:
            info.update(
                {
                    "metadata": change_struct_type(cloud_svc_vo.metadata),
                }
            )

    return cloud_service_pb2.CloudServiceInfo(**info)


def CloudServicesInfo(cloud_svc_vos, total_count, **kwargs):
    return cloud_service_pb2.CloudServicesInfo(
        results=list(map(functools.partial(CloudServiceInfo, **kwargs), cloud_svc_vos)),
        total_count=total_count,
    )


def _change_tags_without_hash(tags) -> dict:
    changed_tags = {}
    for provider, hashed_tags in tags.items():
        for hash_key, tag in hashed_tags.items():
            if not changed_tags.get(provider):
                changed_tags[provider] = {}
            changed_tags[provider][tag["key"]] = tag["value"]

    return changed_tags
