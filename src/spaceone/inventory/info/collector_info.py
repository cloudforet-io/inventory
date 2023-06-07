import logging
import functools
from spaceone.api.inventory.v1 import collector_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils

__all__ = ['CollectorInfo', 'CollectorsInfo', 'VerifyInfo', 'ScheduleInfo', 'SchedulesInfo']

_LOGGER = logging.getLogger(__name__)


def ScheduledInfo(schedule_vo):
    if schedule_vo:
        info = {
            'cron': schedule_vo.cron,
            'interval': schedule_vo.interval,
            'hours': schedule_vo.hours,
            'minutes': schedule_vo.minutes
        }
        return collector_pb2.Scheduled(**info)
    else:
        return None


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
            'metadata': change_struct_type(vo.metadata),
            'upgrade_mode': vo.upgrade_mode,
            'secret_filter': change_struct_type(vo.secret_filter),
        })
    return collector_pb2.PluginInfo(**info)


def CollectorInfo(vo, minimal=False):
    info = {
        'collector_id': vo.collector_id,
        'name': vo.name,
        'state': vo.state,
        'provider': vo.provider,
        'capability': change_struct_type(vo.capability),
        'plugin_info': PluginInfo(vo.plugin_info, minimal=minimal),
    }

    if not minimal:
        info.update({
            'schedule': ScheduledInfo(vo.schedule),
            'created_at': utils.datetime_to_iso8601(vo.created_at),
            'last_collected_at': utils.datetime_to_iso8601(vo.last_collected_at),
            'tags': change_struct_type(vo.tags),
            'domain_id': vo.domain_id
        })

    return collector_pb2.CollectorInfo(**info)


def VerifyInfo(info):
    return collector_pb2.VerifyInfo(**info)


def CollectorsInfo(vos, total_count, **kwargs):
    return collector_pb2.CollectorsInfo(results=list(map(functools.partial(CollectorInfo, **kwargs), vos)),
                                        total_count=total_count)


def ScheduleInfo(vo, minimal=False):
    info = {
        'schedule_id': vo.schedule_id,
        'collect_mode': vo.collect_mode,
        'name': vo.name,
        'schedule': ScheduledInfo(vo.schedule) if vo.schedule else None,
        'collector_info': CollectorInfo(vo.collector, minimal=minimal) if vo.collector else None
    }

    if not minimal:
        info.update({
            'created_at': utils.datetime_to_iso8601(vo.created_at),
            'last_scheduled_at': utils.datetime_to_iso8601(vo.last_scheduled_at),
            'filter': change_struct_type(vo.filters)
        })

    # Temporary code for DB migration
    if not vo.collector_id and vo.collector:
        vo.update({'collector_id': vo.collector.collector_id})

    return collector_pb2.ScheduleInfo(**info)


def SchedulesInfo(vos, total_count, **kwargs):
    return collector_pb2.SchedulesInfo(results=list(map(functools.partial(ScheduleInfo, **kwargs), vos)),
                                       total_count=total_count)
