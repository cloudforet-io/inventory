import logging
import functools
from spaceone.api.inventory.v1 import collector_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils

__all__ = ["CollectorInfo", "CollectorsInfo", "VerifyInfo"]

_LOGGER = logging.getLogger(__name__)


def ScheduledInfo(schedule_vo):
    if schedule_vo:
        info = {"state": schedule_vo.state, "hours": schedule_vo.hours}
        return collector_pb2.Scheduled(**info)
    else:
        return None


def SecretFilterInfo(secret_filter_vo):
    if secret_filter_vo:
        info = {
            "state": secret_filter_vo.state,
            "secrets": secret_filter_vo.secrets,
            "service_accounts": secret_filter_vo.service_accounts,
            "schemas": secret_filter_vo.schemas,
            "exclude_secrets": secret_filter_vo.exclude_secrets,
            "exclude_service_accounts": secret_filter_vo.exclude_service_accounts,
            "exclude_schemas": secret_filter_vo.exclude_schemas,
        }
        return collector_pb2.SecretFilter(**info)
    else:
        return None


def PluginInfo(vo, minimal=False):
    if vo is None:
        return None

    info = {
        "plugin_id": vo.plugin_id,
        "version": vo.version,
    }

    if not minimal:
        info.update(
            {
                "options": change_struct_type(vo.options),
                "metadata": change_struct_type(vo.metadata),
                "upgrade_mode": vo.upgrade_mode,
            }
        )
    return collector_pb2.PluginInfo(**info)


def CollectorInfo(vo, minimal=False):
    info = {
        "collector_id": vo.collector_id,
        "name": vo.name,
        "provider": vo.provider,
        "capability": change_struct_type(vo.capability),
        "plugin_info": PluginInfo(vo.plugin_info, minimal=minimal),
    }

    if not minimal:
        info.update(
            {
                "schedule": ScheduledInfo(vo.schedule),
                "secret_filter": SecretFilterInfo(vo.secret_filter),
                "created_at": utils.datetime_to_iso8601(vo.created_at),
                "last_collected_at": utils.datetime_to_iso8601(vo.last_collected_at),
                "tags": change_struct_type(vo.tags),
                "resource_group": vo.resource_group,
                "workspace_id": vo.workspace_id,
                "domain_id": vo.domain_id,
            }
        )

    return collector_pb2.CollectorInfo(**info)


def VerifyInfo(info):
    return collector_pb2.VerifyInfo(**info)


def CollectorsInfo(vos, total_count, **kwargs):
    return collector_pb2.CollectorsInfo(
        results=list(map(functools.partial(CollectorInfo, **kwargs), vos)),
        total_count=total_count,
    )
