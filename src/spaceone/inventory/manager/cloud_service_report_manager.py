import logging
import copy
from typing import Tuple
from datetime import datetime

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_report_model import CloudServiceReport
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager
from spaceone.inventory.manager.export_manager.email_manager import EmailManager

_LOGGER = logging.getLogger(__name__)


class CloudServiceReportManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_report_model: CloudServiceReport = self.locator.get_model(
            "CloudServiceReport"
        )
        self.cloud_svc_mgr: CloudServiceManager = self.locator.get_manager(
            "CloudServiceManager"
        )

    def create_cloud_service_report(self, params: dict) -> CloudServiceReport:
        def _rollback(vo: CloudServiceReport):
            _LOGGER.info(
                f"[ROLLBACK] Delete cloud service report : {vo.name} ({vo.report_id})"
            )
            vo.delete()

        options = copy.deepcopy(params.get("options", []))
        timezone = params["timezone"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        self.cloud_svc_mgr.get_export_query_results(
            options, timezone, domain_id, workspace_id
        )

        cloud_svc_report_vo: CloudServiceReport = self.cloud_svc_report_model.create(
            params
        )
        self.transaction.add_rollback(_rollback, cloud_svc_report_vo)

        return cloud_svc_report_vo

    def update_cloud_service_report_by_vo(
        self, params: dict, cloud_svc_report_vo: CloudServiceReport
    ) -> CloudServiceReport:
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("report_id")}')
            cloud_svc_report_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cloud_svc_report_vo.to_dict())

        return cloud_svc_report_vo.update(params)

    @staticmethod
    def delete_cloud_service_report_by_vo(
        cloud_svc_report_vo: CloudServiceReport,
    ) -> None:
        cloud_svc_report_vo.delete()

    def get_cloud_service_report(
        self,
        report_id: str,
        domain_id: str,
        workspace_id: str = None,
    ):
        conditions = {
            "report_id": report_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        return self.cloud_svc_report_model.get(**conditions)

    def filter_cloud_service_reports(self, **conditions) -> QuerySet:
        return self.cloud_svc_report_model.filter(**conditions)

    def list_cloud_service_reports(self, query: dict) -> Tuple[QuerySet, int]:
        return self.cloud_svc_report_model.query(**query)

    def stat_cloud_service_reports(self, query: dict) -> dict:
        return self.cloud_svc_report_model.stat(**query)

    def send_cloud_service_report(
        self, cloud_svc_report_vo: CloudServiceReport
    ) -> None:
        options = copy.deepcopy(cloud_svc_report_vo.options)
        file_format = cloud_svc_report_vo.file_format
        name = cloud_svc_report_vo.name
        target = cloud_svc_report_vo.target
        language = cloud_svc_report_vo.language
        timezone = cloud_svc_report_vo.timezone
        resource_group = cloud_svc_report_vo.resource_group
        workspace_id = cloud_svc_report_vo.workspace_id
        domain_id = cloud_svc_report_vo.domain_id

        email_mgr: EmailManager = self.locator.get_manager(
            EmailManager, file_format=file_format, file_name=name
        )

        if resource_group == "DOMAIN":
            self.cloud_svc_mgr.get_export_query_results(options, timezone, domain_id)
            workspace_id = None
        else:
            self.cloud_svc_mgr.get_export_query_results(
                options, timezone, domain_id, workspace_id
            )

        email_mgr.export(
            options,
            domain_id,
            workspace_id=workspace_id,
            name=name,
            target=target,
            language=language,
        )

        cloud_svc_report_vo.update({"last_sent_at": datetime.utcnow()})
