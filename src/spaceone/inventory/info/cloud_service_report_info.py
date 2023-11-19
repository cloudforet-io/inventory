import functools
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_report_model import CloudServiceReport, ReportSchedule

__all__ = ['CloudServiceReportInfo', 'CloudServiceReportsInfo']


def ReportScheduleInfo(schedule_vo: ReportSchedule):
    if schedule_vo is None:
        return None
    else:
        return {
            'state': schedule_vo.state,
            'hours': schedule_vo.hours,
            'days_of_week': schedule_vo.days_of_week
        }


def CloudServiceReportInfo(cloud_svc_report_vo: CloudServiceReport, minimal=False):
    info = {
        'report_id': cloud_svc_report_vo.report_id,
        'name': cloud_svc_report_vo.name,
        'file_format': cloud_svc_report_vo.file_format,
        'schedule': ReportScheduleInfo(cloud_svc_report_vo.schedule),
        'last_sent_at': utils.datetime_to_iso8601(cloud_svc_report_vo.last_sent_at)
    }

    if not minimal:
        info.update({
            'options': [],
            'target': cloud_svc_report_vo.target,
            'language': cloud_svc_report_vo.language,
            'timezone': cloud_svc_report_vo.timezone,
            'tags': cloud_svc_report_vo.tags,
            'domain_id': cloud_svc_report_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(cloud_svc_report_vo.created_at)
        })

        for option in cloud_svc_report_vo.options:
            info['options'].append(option)

    return info


def CloudServiceReportsInfo(cloud_svc_report_vos, total_count, **kwargs):
    return {
        'results': list(map(functools.partial(CloudServiceReportInfo, **kwargs), cloud_svc_report_vos)),
        'total_count': total_count
    }
