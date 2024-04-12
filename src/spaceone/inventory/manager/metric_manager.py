import logging
import copy
from typing import Tuple
from datetime import datetime
from dateutil.relativedelta import relativedelta

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.core import utils, cache
from spaceone.inventory.error.metric import (
    ERROR_NOT_SUPPORT_RESOURCE_TYPE,
    ERROR_METRIC_QUERY_RUN_FAILED,
)
from spaceone.inventory.model.metric.database import Metric
from spaceone.inventory.manager.managed_resource_manager import ManagedResourceManager
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager
from spaceone.inventory.manager.metric_data_manager import MetricDataManager

_LOGGER = logging.getLogger(__name__)


class MetricManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metric_model = Metric
        self.metric_data_mgr = MetricDataManager()

    def create_metric(self, params: dict) -> Metric:
        def _rollback(vo: Metric):
            _LOGGER.info(f"[create_metric._rollback] " f"Delete metric: {vo.metric_id}")
            vo.delete()

        if "metric_id" not in params:
            params["metric_id"] = utils.generate_id("metric")

        params["workspaces"] = [params["workspace_id"]]
        params["label_keys"] = self._get_label_keys(params["query_options"])

        metric_vo: Metric = self.metric_model.create(params)
        self.transaction.add_rollback(_rollback, metric_vo)

        return metric_vo

    def update_metric_by_vo(self, params: dict, metric_vo: Metric) -> Metric:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_metric_by_vo._rollback] Revert Data: "
                f'{old_data["metric_id"]}'
            )
            metric_vo.update(old_data)

        if "query_options" in params:
            params["label_keys"] = self._get_label_keys(params["query_options"])

        self.transaction.add_rollback(_rollback, metric_vo.to_dict())

        return metric_vo.update(params)

    @staticmethod
    def delete_metric_by_vo(metric_vo: Metric) -> None:
        metric_vo.delete()

    def get_metric(
        self,
        metric_id: str,
        domain_id: str,
        workspace_id: str = None,
    ) -> Metric:
        conditions = {
            "metric_id": metric_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspaces"] = [workspace_id, "*"]

        return self.metric_model.get(**conditions)

    def filter_metrics(self, **conditions) -> QuerySet:
        return self.metric_model.filter(**conditions)

    def list_metrics(self, query: dict, domain_id: str) -> Tuple[QuerySet, int]:
        self._create_managed_metric(domain_id)
        return self.metric_model.query(**query)

    def stat_metrics(self, query: dict) -> dict:
        return self.metric_model.stat(**query)

    def run_metric_query(self, metric_vo: Metric, workspace_id: str = None) -> None:
        query_options = metric_vo.query_options
        resource_type = metric_vo.resource_type
        domain_id = metric_vo.domain_id

        results = self.analyze_resource(
            query_options, resource_type, domain_id, workspace_id
        )

        created_at = datetime.utcnow()

        try:
            for result in results:
                self._save_query_result(metric_vo, result, created_at)
            self._delete_changed_metric_data(metric_vo, created_at)

            if metric_vo.metric_type == "COUNTER":
                self._aggregate_monthly_metric_data(metric_vo, created_at)

            self._delete_changed_monthly_metric_data(metric_vo, created_at)
        except Exception as e:
            _LOGGER.error(
                f"[run_metric_query] Failed to save query result: {e}",
                exc_info=True,
            )
            self._rollback_query_results(metric_vo, created_at)
            raise ERROR_METRIC_QUERY_RUN_FAILED(metric_id=metric_vo.metric_id)

        self._update_status(metric_vo, created_at)
        self._delete_invalid_metric_data(metric_vo)
        self._delete_old_metric_data(metric_vo)
        self._remove_analyze_cache(metric_vo.domain_id, metric_vo.metric_id)

    def analyze_resource(
        self, query: dict, resource_type: str, domain_id: str, workspace_id: str = None
    ) -> list:
        if resource_type == "inventory.CloudService":
            return self._analyze_cloud_service(query, domain_id, workspace_id)
        else:
            raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=resource_type)

    @staticmethod
    def _analyze_cloud_service(query: dict, domain_id: str, workspace_id: str) -> list:
        analyze_query = copy.deepcopy(query)
        analyze_query["group_by"] += [
            "project_id",
            "workspace_id",
            "domain_id",
        ]
        analyze_query["group_by"] = list(set(analyze_query["group_by"]))

        if workspace_id:
            analyze_query["filter"] = analyze_query.get("filter", [])
            analyze_query["filter"].append(
                {"k": "workspace_id", "v": workspace_id, "o": "eq"}
            )

        if "select" in analyze_query:
            for group_by_key in ["project_id", "workspace_id", "domain_id"]:
                analyze_query["select"][group_by_key] = group_by_key

        _LOGGER.debug(f"[_analyze_cloud_service] Analyze Query: {analyze_query}")
        cloud_svc_mgr = CloudServiceManager()
        response = cloud_svc_mgr.analyze_cloud_services(
            analyze_query, change_filter=True, domain_id=domain_id
        )
        return response.get("results", [])

    @cache.cacheable(key="inventory:managed-metric:{domain_id}:sync", expire=300)
    def _create_managed_metric(self, domain_id: str) -> bool:
        managed_resource_mgr = ManagedResourceManager()

        metric_vos = self.filter_metrics(domain_id=domain_id, is_managed=True)

        installed_metric_version_map = {}
        for metric_vo in metric_vos:
            installed_metric_version_map[metric_vo.metric_id] = metric_vo.version

        managed_metric_map = managed_resource_mgr.get_managed_metrics()

        for managed_metric_id, managed_metric_info in managed_metric_map.items():
            managed_metric_info["domain_id"] = domain_id
            managed_metric_info["is_managed"] = True
            managed_metric_info["workspace_id"] = "*"

            if metric_version := installed_metric_version_map.get(managed_metric_id):
                if metric_version != managed_metric_info["version"]:
                    _LOGGER.debug(
                        f"[_create_managed_metric] update managed metric: {managed_metric_id}"
                    )
                    metric_vo = self.get_metric(managed_metric_id, domain_id)
                    self.update_metric_by_vo(managed_metric_info, metric_vo)
            else:
                _LOGGER.debug(
                    f"[_create_managed_metric] create new managed metric: {managed_metric_id}"
                )
                self.create_metric(managed_metric_info)

        return True

    def _save_query_result(
        self, metric_vo: Metric, result: dict, created_at: datetime
    ) -> None:
        data = {
            "metric_id": metric_vo.metric_id,
            "value": result["value"],
            "unit": metric_vo.unit,
            "labels": {},
            "namespace_id": metric_vo.namespace_id,
            "project_id": result.get("project_id"),
            "workspace_id": result["workspace_id"],
            "domain_id": metric_vo.domain_id,
            "created_year": created_at.strftime("%Y"),
            "created_month": created_at.strftime("%Y-%m"),
            "created_date": created_at.strftime("%Y-%m-%d"),
        }

        for key, value in result.items():
            if key not in ["project_id", "workspace_id", "domain_id", "value"]:
                data["labels"][key] = value

        self.metric_data_mgr.create_metric_data(data)

        if metric_vo.metric_type == "GAUGE":
            self.metric_data_mgr.create_monthly_metric_data(data)

    def _aggregate_monthly_metric_data(
        self, metric_vo: Metric, created_at: datetime
    ) -> None:
        domain_id = metric_vo.domain_id
        metric_id = metric_vo.metric_id
        created_month = created_at.strftime("%Y-%m")
        created_year = created_at.strftime("%Y")
        group_by = ["project_id", "workspace_id"]

        for key in metric_vo.label_keys:
            group_by.append(f"labels.{key}")

        query = {
            "group_by": group_by,
            "fields": {
                "value": {
                    "key": "value",
                    "operator": "sum",
                }
            },
            "filter": [
                {"k": "metric_id", "v": metric_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "created_month", "v": created_month, "o": "eq"},
            ],
        }

        response = self.metric_data_mgr.analyze_metric_data(query, target="PRIMARY")
        for result in response.get("results", []):
            data = {
                "metric_id": metric_vo.metric_id,
                "value": result["value"],
                "unit": metric_vo.unit,
                "labels": {},
                "namespace_id": metric_vo.namespace_id,
                "project_id": result.get("project_id"),
                "workspace_id": result["workspace_id"],
                "domain_id": metric_vo.domain_id,
                "created_year": created_year,
                "created_month": created_month,
            }

            for key, value in result.items():
                if key not in ["project_id", "workspace_id", "domain_id", "value"]:
                    data["labels"][key] = value

            self.metric_data_mgr.create_monthly_metric_data(data)

    def _delete_changed_metric_data(
        self, metric_vo: Metric, created_at: datetime
    ) -> None:
        domain_id = metric_vo.domain_id
        metric_id = metric_vo.metric_id
        created_date = created_at.strftime("%Y-%m-%d")

        metric_data_vos = self.metric_data_mgr.filter_metric_data(
            metric_id=metric_id,
            domain_id=domain_id,
            created_date=created_date,
            status="DONE",
        )

        _LOGGER.debug(
            f"[_delete_changed_metric_data] delete count: {metric_data_vos.count()}"
        )
        metric_data_vos.delete()

    def _delete_changed_monthly_metric_data(
        self, metric_vo: Metric, created_at: datetime
    ):
        domain_id = metric_vo.domain_id
        metric_id = metric_vo.metric_id
        created_month = created_at.strftime("%Y-%m")

        monthly_metric_data_vos = self.metric_data_mgr.filter_monthly_metric_data(
            metric_id=metric_id,
            domain_id=domain_id,
            created_month=created_month,
            status="DONE",
        )

        _LOGGER.debug(
            f"[_delete_changed_monthly_metric_data] delete count: {monthly_metric_data_vos.count()}"
        )
        monthly_metric_data_vos.delete()

    def _rollback_query_results(self, metric_vo: Metric, created_at: datetime):
        _LOGGER.debug(
            f"[_rollback_query_results] Rollback Query Results: {metric_vo.metric_id}"
        )
        metric_id = metric_vo.metric_id
        domain_id = metric_vo.domain_id

        metric_data_vos = self.metric_data_mgr.filter_metric_data(
            metric_id=metric_id,
            domain_id=domain_id,
            created_date=created_at.strftime("%Y-%m-%d"),
            status="IN_PROGRESS",
        )
        metric_data_vos.delete()

        monthly_metric_data_vos = self.metric_data_mgr.filter_monthly_metric_data(
            metric_id=metric_id,
            domain_id=domain_id,
            created_month=created_at.strftime("%Y-%m"),
            status="IN_PROGRESS",
        )
        monthly_metric_data_vos.delete()

    def _update_status(self, metric_vo: Metric, created_at: datetime) -> None:
        domain_id = metric_vo.domain_id
        metric_id = metric_vo.metric_id
        created_date = created_at.strftime("%Y-%m-%d")
        created_month = created_at.strftime("%Y-%m")

        metric_data_vos = self.metric_data_mgr.filter_metric_data(
            metric_id=metric_id,
            domain_id=domain_id,
            created_date=created_date,
            status="IN_PROGRESS",
        )
        metric_data_vos.update({"status": "DONE"})

        monthly_metric_data_vos = self.metric_data_mgr.filter_monthly_metric_data(
            metric_id=metric_id,
            domain_id=domain_id,
            created_month=created_month,
            status="IN_PROGRESS",
        )
        monthly_metric_data_vos.update({"status": "DONE"})

    def _delete_invalid_metric_data(self, metric_vo: Metric) -> None:
        domain_id = metric_vo.domain_id
        metric_id = metric_vo.metric_id

        metric_data_vos = self.metric_data_mgr.filter_metric_data(
            metric_id=metric_id, domain_id=domain_id, status="IN_PROGRESS"
        )

        if metric_data_vos.count() > 0:
            _LOGGER.debug(
                f"[_delete_invalid_metric_data] delete metric data count: {metric_data_vos.count()}"
            )
            metric_data_vos.delete()

        monthly_metric_data_vos = self.metric_data_mgr.filter_monthly_metric_data(
            metric_id=metric_id, domain_id=domain_id, status="IN_PROGRESS"
        )

        if monthly_metric_data_vos.count() > 0:
            _LOGGER.debug(
                f"[_delete_invalid_metric_data] delete monthly metric data count: {monthly_metric_data_vos.count()}"
            )
            monthly_metric_data_vos.delete()

    def _delete_old_metric_data(self, metric_vo: Metric) -> None:
        now = datetime.utcnow().date()
        domain_id = metric_vo.domain_id
        metric_id = metric_vo.metric_id
        old_created_month = (now - relativedelta(months=12)).strftime("%Y-%m")
        old_created_year = (now - relativedelta(months=36)).strftime("%Y")

        delete_query = {
            "filter": [
                {"k": "created_month", "v": old_created_month, "o": "lt"},
                {"k": "metric_id", "v": metric_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ]
        }

        metric_data_vos, total_count = self.metric_data_mgr.list_metric_data(
            delete_query
        )

        if total_count > 0:
            _LOGGER.debug(
                f"[_delete_old_metric_data] delete metric data count: {total_count}"
            )
            metric_data_vos.delete()

        monthly_delete_query = {
            "filter": [
                {"k": "created_year", "v": old_created_year, "o": "lt"},
                {"k": "metric_id", "v": metric_id, "o": "eq"},
                {"k": "domain_id", "v": domain_id, "o": "eq"},
            ]
        }

        (
            monthly_metric_data_vos,
            total_count,
        ) = self.metric_data_mgr.list_monthly_metric_data(monthly_delete_query)

        if total_count > 0:
            _LOGGER.debug(
                f"[_delete_old_metric_data] delete monthly metric data count: {total_count}"
            )
            monthly_metric_data_vos.delete()

    @staticmethod
    def _remove_analyze_cache(domain_id: str, metric_id: str) -> None:
        cache.delete_pattern(f"inventory:metric-data:*:{domain_id}:{metric_id}:*")
        cache.delete_pattern(f"inventory:metric-data:{domain_id}:{metric_id}:*")

    @staticmethod
    def _get_label_keys(query_options: dict) -> list:
        label_keys = []
        for key in query_options.get("group_by", []):
            if key not in ["project_id", "workspace_id", "domain_id"]:
                label_keys.append(key.rsplit(".", 1)[-1])
        return label_keys
