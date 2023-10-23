import logging
import copy
from datetime import datetime

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_report_model import CloudServiceReport
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager
from spaceone.inventory.manager.export_manager.email_manager import EmailManager

_LOGGER = logging.getLogger(__name__)


class CloudServiceReportManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_report_model: CloudServiceReport = self.locator.get_model('CloudServiceReport')
        self.cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')

    def create_cloud_service_report(self, params):
        def _rollback(cloud_svc_report_vo: CloudServiceReport):
            _LOGGER.info(
                f'[ROLLBACK] Delete Cloud Service Report : {cloud_svc_report_vo.name} ({cloud_svc_report_vo.report_id})')
            cloud_svc_report_vo.delete()

        options = copy.deepcopy(params.get('options', []))
        domain_id = params['domain_id']

        self.cloud_svc_mgr.get_export_query_results(options, domain_id)

        cloud_svc_report_vo: CloudServiceReport = self.cloud_svc_report_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_report_vo)

        return cloud_svc_report_vo

    def update_cloud_service_report(self, params):
        return self.update_cloud_service_report_by_vo(
            params, self.get_cloud_service_report(params['report_id'], params['domain_id']))

    def update_cloud_service_report_by_vo(self, params, cloud_svc_report_vo: CloudServiceReport):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("report_id")}')
            cloud_svc_report_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cloud_svc_report_vo.to_dict())

        return cloud_svc_report_vo.update(params)

    def delete_cloud_service_report(self, report_id, domain_id):
        self.delete_cloud_service_report_by_vo(self.get_cloud_service_report(report_id, domain_id))

    def delete_cloud_service_report_by_vo(self, cloud_svc_report_vo: CloudServiceReport):
        cloud_svc_report_vo.delete()

    def get_cloud_service_report(self, report_id, domain_id, only=None):
        return self.cloud_svc_report_model.get(report_id=report_id, domain_id=domain_id, only=only)

    def filter_cloud_service_reports(self, **conditions):
        return self.cloud_svc_report_model.filter(**conditions)

    def list_cloud_service_reports(self, query):
        return self.cloud_svc_report_model.query(**query)

    def stat_cloud_service_reports(self, query):
        return self.cloud_svc_report_model.stat(**query)

    def send_cloud_service_report(self, cloud_svc_report_vo: CloudServiceReport):
        options = copy.deepcopy(cloud_svc_report_vo.options)
        domain_id = cloud_svc_report_vo.domain_id
        file_format = cloud_svc_report_vo.file_format
        name = cloud_svc_report_vo.name
        target = cloud_svc_report_vo.target

        email_mgr: EmailManager = self.locator.get_manager(EmailManager,
                                                           file_format=file_format,
                                                           file_name=name)

        self.cloud_svc_mgr.get_export_query_results(options, domain_id)

        email_mgr.export(options, domain_id, name=name, target=target)

        cloud_svc_report_vo.update({
            'last_sent_at': datetime.utcnow()
        })
