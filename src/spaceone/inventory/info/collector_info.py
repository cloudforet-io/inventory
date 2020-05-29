import logging
import functools

from spaceone.api.inventory.v1 import collector_pb2
from spaceone.core.pygrpc.message_type import *

__all__ = ['CollectorInfo', 'CollectorsInfo', 'VerifyInfo', 'ScheduleInfo', 'SchedulesInfo']

_LOGGER = logging.getLogger(__name__)


def PluginInfo(vo, minimal=False):
    if vo is None:
        return None

    info = {
        'plugin_id': vo.plugin_id,
        'version': vo.version,
    }

    if not minimal:
        info.update({
            'options': change_struct_type(vo.options),
            'secret_id': vo.secret_id,
            'secret_group_id': vo.secret_group_id,
            'provider': vo.provider
        })
    return collector_pb2.PluginInfo(**info)


def CollectorInfo(vo, minimal=False):
    info = {
        'collector_id': vo.collector_id,
        'name': vo.name,
        'provider': vo.provider,
        'capability': change_struct_type(vo.capability),
        'plugin_info': PluginInfo(vo.plugin_info, minimal=minimal),
        'state': vo.state
    }

    if not minimal:
        info.update({
            'priority': vo.priority,
            'created_at': change_timestamp_type(vo.created_at),
            'last_collected_at': change_timestamp_type(vo.last_collected_at),
            'tags': change_struct_type(vo.tags),
            'domain_id': vo.domain_id
        })

    return collector_pb2.CollectorInfo(**info)


def VerifyInfo(info):
    return collector_pb2.VerifyInfo(**info)


def CollectorsInfo(vos, total_count, **kwargs):
    return collector_pb2.CollectorsInfo(results=list(map(functools.partial(CollectorInfo, **kwargs), vos)),
                                        total_count=total_count)


def ScheduledInfo(vo):
    info = {
        'cron': vo.cron,
        'interval': vo.interval,
        'hours': vo.hours,
        'minutes': vo.minutes
    }
    return collector_pb2.Scheduled(**info)


def ScheduleInfo(vo, minimal=False):
    info = {
        'schedule_id': vo.schedule_id,
        'collect_mode': vo.collect_mode,
        'name': vo.name,
        'schedule': ScheduledInfo(vo.schedule),
        'collector_info': CollectorInfo(vo.collector, minimal=minimal) if vo.collector else None
    }

    if not minimal:
        info.update({
            'created_at': change_timestamp_type(vo.created_at),
            'last_scheduled_at': change_timestamp_type(vo.last_scheduled_at),
            'filter': change_struct_type(vo.filters)
        })
    return collector_pb2.ScheduleInfo(**info)


def SchedulesInfo(vos, total_count, **kwargs):
    return collector_pb2.SchedulesInfo(results=list(map(functools.partial(ScheduleInfo, **kwargs), vos)),
                                       total_count=total_count)
