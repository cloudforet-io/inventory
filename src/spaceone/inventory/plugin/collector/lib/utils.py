import logging
import json

from spaceone.core.error import *
from spaceone.inventory.plugin.collector.error.response import ERROR_INVALID_PARAMETER
from spaceone.inventory.plugin.collector.model import (
    CloudService,
    CloudServiceType,
    Reference,
)
from spaceone.inventory.plugin.collector.lib.metadata import (
    convert_cloud_service_type_meta,
    convert_cloud_service_meta,
)

_LOGGER = logging.getLogger(__name__)

VALID_RESOURCE_TYPES = {
    "cloud_service_type": "inventory.CloudServiceType",
    "cloud_service": "inventory.CloudService",
    "region": "inventory.Region",
}


def make_cloud_service_type(
    name,
    group,
    provider,
    metadata_path,
    is_primary=False,
    is_major=False,
    service_code=None,
    tags=None,
    labels=None,
) -> dict:
    if tags is None:
        tags = {}
    if labels is None:
        labels = []

    cloud_service_type = CloudServiceType(
        name=name,
        group=group,
        provider=provider,
        metadata=convert_cloud_service_type_meta(metadata_path),
        is_primary=is_primary,
        is_major=is_major,
        service_code=service_code,
        tags=tags,
        labels=labels,
    )

    return cloud_service_type.dict()


def make_cloud_service(
    name: str,
    cloud_service_type: str,
    cloud_service_group: str,
    provider: str,
    data: dict,
    ip_addresses: list = None,
    account: str = None,
    instance_type: str = None,
    instance_size: float = None,
    region_code: str = None,
    reference: Reference = None,
    tags: dict = None,
) -> dict:
    if ip_addresses is None:
        ip_addresses = []
    if instance_size is None:
        instance_size = 0
    if tags is None:
        tags = {}

    cloud_service = CloudService(
        name=name,
        cloud_service_type=cloud_service_type,
        cloud_service_group=cloud_service_group,
        provider=provider,
        data=data,
        metadata=convert_cloud_service_meta(
            provider, cloud_service_group, cloud_service_type
        ),
        ip_addresses=ip_addresses,
        account=account,
        instance_type=instance_type,
        instance_size=instance_size,
        region_code=region_code,
        reference=reference,
        tags=tags,
    )

    return cloud_service.dict()


def make_response(
    match_keys: list,
    cloud_service_type=None,
    cloud_service=None,
    region=None,
    metric=None,
    namespace=None,
    resource_type: str = "inventory.CloudService",
) -> dict:
    response = {
        "state": "SUCCESS",
        "resource_type": resource_type,
        "match_keys": match_keys,
    }

    if resource_type == "inventory.CloudServiceType" and cloud_service_type is not None:
        response["cloud_service_type"] = cloud_service_type
        return response

    elif resource_type == "inventory.CloudService" and cloud_service is not None:
        response["cloud_service"] = cloud_service
        return response

    elif resource_type == "inventory.Region" and region is not None:
        response["region"] = region
        return response
    elif resource_type == "inventory.Metric" and metric is not None:
        response["metric"] = metric
        return response
    elif resource_type == "inventory.Namespace" and namespace is not None:
        response["namespace"] = namespace
        return response
    else:
        # TODO: Check this logic
        raise ERROR_INVALID_PARAMETER()


def make_error_response(
    error: Exception,
    provider: str,
    cloud_service_group: str,
    cloud_service_type: str,
    resource_type: str = "inventory.CloudService",
    region_name: str = "",
) -> dict:
    if isinstance(error, ERROR_BASE):
        error = ERROR_UNKNOWN(message=error)
        error_message = error.message
    elif type(error) is dict:
        error_message = json.dumps(error)
    else:
        error_message = str(error)

    _LOGGER.error(
        f"[error_response] {cloud_service_group} / {cloud_service_type}",
        exc_info=True,
    )
    return {
        "state": "FAILURE",
        "resource_type": "inventory.ErrorResource",
        "error_message": error_message,
        "error_data": {
            "provider": provider,
            "cloud_service_group": cloud_service_group,
            "cloud_service_type": cloud_service_type,
            "resource_type": resource_type,
        },
    }
