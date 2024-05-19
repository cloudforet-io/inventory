import logging
import copy
import time
import random
from typing import Tuple
from datetime import datetime
from dateutil.relativedelta import relativedelta

from spaceone.core import config, queue
from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.core import utils, cache
from spaceone.inventory.error.metric import (
    ERROR_NOT_SUPPORT_RESOURCE_TYPE,
    ERROR_METRIC_QUERY_RUN_FAILED,
    ERROR_WRONG_QUERY_OPTIONS,
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

    def push_task(self, metric_vo: Metric) -> None:
        metric_id = metric_vo.metric_id
        domain_id = metric_vo.domain_id

        task = {
            "name": "run_metric_query",
            "version": "v1",
            "executionEngine": "BaseWorker",
            "stages": [
                {
                    "locator": "SERVICE",
                    "name": "MetricService",
                    "metadata": {
                        "token": self.transaction.get_meta("token"),
                    },
                    "method": "run_metric_query",
                    "params": {
                        "params": {
                            "metric_id": metric_id,
                            "domain_id": domain_id,
                        }
                    },
                }
            ],
        }

        _LOGGER.debug(f"[push_task] run query sets by domain: {domain_id}")

        queue.put("collector_q", utils.dump_json(task))

    def create_metric(self, params: dict) -> Metric:
        def _rollback(vo: Metric):
            _LOGGER.info(f"[create_metric._rollback] " f"Delete metric: {vo.metric_id}")
            vo.delete()

        if params["metric_type"] == "COUNTER":
            params["date_field"] = params.get("date_field") or "created_at"

        if params.get("metric_id") is None:
            params["metric_id"] = utils.generate_id("metric")

        params["workspaces"] = [params["workspace_id"]]
        params["labels_info"] = self._get_labels_info(params["query_options"])

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
            params["labels_info"] = self._get_labels_info(params["query_options"])

        self.transaction.add_rollback(_rollback, metric_vo.to_dict())

        metric_vo = metric_vo.update(params)

        if "query_options" in params:
            self.metric_data_mgr.delete_metric_data_by_metric_id(
                metric_vo.metric_id, metric_vo.domain_id
            )

        return metric_vo

    def delete_metric_by_vo(self, metric_vo: Metric) -> None:
        metric_id = metric_vo.metric_id
        domain_id = metric_vo.domain_id
        metric_vo.delete()

        self.metric_data_mgr.delete_metric_data_by_metric_id(metric_id, domain_id)

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
        self.create_managed_metric(domain_id)
        return self.metric_model.query(**query)

    def stat_metrics(self, query: dict) -> dict:
        return self.metric_model.stat(**query)

    def check_and_run_metric_query(
        self, metric_id: str, domain_id: str, workspace_id: str = None
    ):
        metric_vo = self.get_metric(metric_id, domain_id, workspace_id)
        plugin_id = metric_vo.plugin_id

        if metric_vo.is_managed:
            is_load: bool = cache.get(
                f"inventory:managed-metric:{domain_id}:{metric_id}:load"
            )
        else:
            is_load: bool = cache.get(
                f"inventory:plugin-metric:{domain_id}:{plugin_id}:{metric_id}:load"
            )

        if not is_load:
            self._wait_random_time()
            metric_vo = self.get_metric(metric_id, domain_id, workspace_id)

            if metric_vo.status == "IN_PROGRESS":
                _LOGGER.debug(
                    f"[check_and_run_metric_query] metric is already in progress: {metric_id}"
                )
                self._check_metric_status(metric_vo)
            else:
                _LOGGER.debug(
                    f"[check_and_run_metric_query] run metric query: {metric_id}"
                )
                self.run_metric_query(metric_vo)

    @staticmethod
    def _wait_random_time():
        random_time = round(random.uniform(0, 5), 2)
        _LOGGER.debug(f"[_wait_random_time] sleep time: {random_time}")

        time.sleep(random_time)

    def _check_metric_status(self, metric_vo: Metric) -> None:
        for i in range(300):
            metric_vo = self.get_metric(metric_vo.metric_id, metric_vo.domain_id)
            if metric_vo.status == "DONE":
                break

            time.sleep(3)

        self.update_metric_by_vo({"status": "DONE"}, metric_vo)

    def run_metric_query(self, metric_vo: Metric, is_yesterday: bool = False) -> None:
        self.update_metric_by_vo({"status": "IN_PROGRESS"}, metric_vo)

        domain_id = metric_vo.domain_id

        results = self.analyze_resource(metric_vo, is_yesterday=is_yesterday)

        created_at = datetime.utcnow()

        if is_yesterday and metric_vo.metric_type == "COUNTER":
            created_at = created_at - relativedelta(days=1)

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
        self._set_metric_load_cache(domain_id, metric_vo)

        self.update_metric_by_vo({"status": "DONE"}, metric_vo)

    def analyze_resource(
        self,
        metric_vo: Metric,
        workspace_id: str = None,
        query_options: dict = None,
        is_yesterday: bool = False,
    ) -> list:
        resource_type = metric_vo.resource_type
        domain_id = metric_vo.domain_id
        query = query_options or metric_vo.query_options
        query = copy.deepcopy(query)
        metric_type = metric_vo.metric_type
        date_field = metric_vo.date_field

        if "*" not in metric_vo.workspaces:
            query = self._append_workspace_filter(query, metric_vo.workspaces)

        if metric_type == "COUNTER":
            query = self._append_datetime_filter(
                query, date_field=date_field, is_yesterday=is_yesterday
            )

        try:
            if metric_vo.resource_type == "inventory.CloudService":
                return self._analyze_cloud_service(query, domain_id, workspace_id)
            elif metric_vo.resource_type.startswith("inventory.CloudService:"):
                cloud_service_type_key = metric_vo.resource_type.split(":")[-1]
                return self._analyze_cloud_service(
                    query, domain_id, workspace_id, cloud_service_type_key
                )
            else:
                raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(resource_type=resource_type)
        except Exception as e:
            _LOGGER.error(
                f"[analyze_resource] Failed to analyze query: {e}",
                exc_info=True,
            )
            raise ERROR_WRONG_QUERY_OPTIONS(
                query_options=utils.dump_json(metric_vo.query_options)
            )

    @staticmethod
    def _append_workspace_filter(query: dict, workspaces: list) -> dict:
        query["filter"] = query.get("filter", [])
        query["filter"].append({"k": "workspace_id", "v": workspaces, "o": "in"})
        return query

    @staticmethod
    def _append_datetime_filter(
        query: dict,
        date_field: str = "created_at",
        is_yesterday: bool = False,
    ) -> dict:
        scheduler_hour = config.get_global("METRIC_SCHEDULE_HOUR", 0)
        end = datetime.utcnow().replace(
            hour=scheduler_hour, minute=0, second=0, microsecond=0
        )

        if is_yesterday:
            end = end - relativedelta(days=1)

        start = end - relativedelta(days=1)

        query["filter"] = query.get("filter", [])
        query["filter"].extend(
            [
                {"k": date_field, "v": start, "o": "gte"},
                {"k": date_field, "v": end, "o": "lt"},
            ]
        )
        return query

    @staticmethod
    def _analyze_cloud_service(
        query: dict,
        domain_id: str,
        workspace_id: str,
        cloud_service_type_key: str = None,
    ) -> list:
        default_group_by = [
            "collection_info.service_account_id",
            "project_id",
            "workspace_id",
        ]
        changed_group_by = []
        changed_group_by += copy.deepcopy(default_group_by)

        for group_option in query.get("group_by", []):
            if isinstance(group_option, dict):
                key = group_option.get("key")
            else:
                key = group_option

            if key not in default_group_by:
                changed_group_by.append(group_option)

        query["group_by"] = changed_group_by
        query["filter"] = query.get("filter", [])
        query["filter"].append({"k": "domain_id", "v": domain_id, "o": "eq"})

        if workspace_id:
            query["filter"].append({"k": "workspace_id", "v": workspace_id, "o": "eq"})

        if cloud_service_type_key:
            try:
                (
                    provider,
                    cloud_service_group,
                    cloud_service_type,
                ) = cloud_service_type_key.split(".")
                query["filter"].append({"k": f"provider", "v": provider, "o": "eq"})
                query["filter"].append(
                    {"k": f"cloud_service_group", "v": cloud_service_group, "o": "eq"}
                )
                query["filter"].append(
                    {"k": f"cloud_service_type", "v": cloud_service_type, "o": "eq"}
                )
            except Exception as e:
                raise ERROR_NOT_SUPPORT_RESOURCE_TYPE(
                    resource_type=f"inventory.CloudService:{cloud_service_type_key}"
                )

        if "select" in query:
            for group_by_key in ["service_account_id", "project_id", "workspace_id"]:
                query["select"][group_by_key] = group_by_key

        _LOGGER.debug(f"[_analyze_cloud_service] Analyze Query: {query}")
        cloud_svc_mgr = CloudServiceManager()
        response = cloud_svc_mgr.analyze_cloud_services(
            query, change_filter=True, domain_id=domain_id
        )
        return response.get("results", [])

    @cache.cacheable(key="inventory:managed-metric:{domain_id}:sync", expire=300)
    def create_managed_metric(self, domain_id: str) -> bool:
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
                        f"[create_managed_metric] update managed metric: {managed_metric_id}"
                    )
                    metric_vo = self.get_metric(managed_metric_id, domain_id)
                    self.update_metric_by_vo(managed_metric_info, metric_vo)
            else:
                _LOGGER.debug(
                    f"[create_managed_metric] create new managed metric: {managed_metric_id}"
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

        for key in metric_vo.labels_info:
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
        cache.delete_pattern(
            f"inventory:metric-query-history:{domain_id}:{metric_id}:*"
        )

    @staticmethod
    def _set_metric_load_cache(domain_id: str, metric_vo: Metric) -> None:
        if metric_vo.is_managed:
            cache_key = (
                f"inventory:managed-metric:{domain_id}:{metric_vo.metric_id}:load"
            )
        else:
            cache_key = f"inventory:plugin-metric:{domain_id}:{metric_vo.plugin_id}:{metric_vo.metric_id}:load"

        cache.set(
            cache_key,
            True,
            expire=3600 * 24,
        )

    @staticmethod
    def _get_labels_info(query_options: dict) -> list:
        query_options = copy.deepcopy(query_options)
        labels_info = [
            {
                "key": "workspace_id",
                "name": "Workspace",
                "reference": {
                    "resource_type": "identity.Workspace",
                    "reference_key": "workspace_id",
                },
            },
            {
                "key": "project_id",
                "name": "Project",
                "reference": {
                    "resource_type": "identity.Project",
                    "reference_key": "project_id",
                },
            },
            {
                "key": "labels.service_account_id",
                "name": "Service Account",
                "search_key": "collection_info.service_account_id",
                "reference": {
                    "resource_type": "identity.ServiceAccount",
                    "reference_key": "service_account_id",
                },
            },
        ]
        for group_option in query_options.get("group_by", []):
            if isinstance(group_option, dict):
                key = group_option.get("key")
                name = group_option.get("name")
                label_info = group_option
            else:
                key = group_option
                name = key.rsplit(".", 1)[-1]
                label_info = {"key": key, "name": name}

            if key not in ["service_account_id", "project_id", "workspace_id"]:
                label_info["key"] = f"labels.{name}"

            if "search_key" not in label_info:
                label_info["search_key"] = key

            labels_info.append(label_info)

        return labels_info
