import logging
import copy
from typing import Tuple
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.core import utils, cache
from spaceone.inventory.error.cloud_service_stats import *
from spaceone.inventory.model.cloud_service_stats_model import (
    CloudServiceStats,
    MonthlyCloudServiceStats,
    CloudServiceStatsQueryHistory,
)

_LOGGER = logging.getLogger(__name__)


class CloudServiceStatsManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_stats_model: CloudServiceStats = self.locator.get_model(
            "CloudServiceStats"
        )
        self.monthly_stats_model: MonthlyCloudServiceStats = self.locator.get_model(
            "MonthlyCloudServiceStats"
        )

    def create_cloud_service_stats(
        self, params: dict, execute_rollback: bool = True
    ) -> CloudServiceStats:
        def _rollback(vo: CloudServiceStats):
            _LOGGER.info(
                f"[create_cloud_service_stats_data._rollback] "
                f"Delete stats data : {vo.query_set_id}"
            )
            vo.delete()

        cloud_svc_stats_vo: CloudServiceStats = self.cloud_svc_stats_model.create(
            params
        )

        if execute_rollback:
            self.transaction.add_rollback(_rollback, cloud_svc_stats_vo)

        return cloud_svc_stats_vo

    def create_monthly_cloud_service_stats(
        self, params: dict, execute_rollback: bool = True
    ) -> MonthlyCloudServiceStats:
        def _rollback(vo: MonthlyCloudServiceStats):
            _LOGGER.info(
                f"[create_monthly_cloud_service_stats._rollback] "
                f"Delete stats data : {vo.query_set_id}"
            )
            vo.delete()

        monthly_stats_vo: MonthlyCloudServiceStats = self.monthly_stats_model.create(
            params
        )

        if execute_rollback:
            self.transaction.add_rollback(_rollback, monthly_stats_vo)

        return monthly_stats_vo

    def filter_cloud_service_stats(self, **conditions) -> QuerySet:
        return self.cloud_svc_stats_model.filter(**conditions)

    def filter_monthly_cloud_service_stats(self, **conditions) -> QuerySet:
        return self.monthly_stats_model.filter(**conditions)

    def list_cloud_service_stats(self, query: dict) -> Tuple[QuerySet, int]:
        query = self._append_status_filter(query)
        return self.cloud_svc_stats_model.query(**query)

    def list_monthly_cloud_service_stats(self, query: dict) -> Tuple[QuerySet, int]:
        query = self._append_status_filter(query)
        return self.monthly_stats_model.query(**query)

    def stat_cloud_service_stats(self, query: dict) -> dict:
        query = self._append_status_filter(query)
        return self.cloud_svc_stats_model.stat(**query)

    def stat_monthly_cloud_service_stats(self, query: dict) -> dict:
        query = self._append_status_filter(query)
        return self.monthly_stats_model.stat(**query)

    def analyze_cloud_service_stats(
        self, query: dict, target: str = "SECONDARY_PREFERRED"
    ) -> dict:
        query["target"] = target
        query["date_field"] = "created_date"
        query["date_field_format"] = "%Y-%m-%d"
        _LOGGER.debug(f"[analyze_cloud_service_stats] query: {query}")
        return self.cloud_svc_stats_model.analyze(**query)

    def analyze_monthly_cloud_service_stats(
        self, query: dict, target: str = "SECONDARY_PREFERRED"
    ) -> dict:
        query["target"] = target
        query["date_field"] = "created_month"
        query["date_field_format"] = "%Y-%m"
        _LOGGER.debug(f"[analyze_monthly_cloud_service_stats] query: {query}")
        return self.monthly_stats_model.analyze(**query)

    def analyze_yearly_cloud_service_stats(
        self, query: dict, target: str = "SECONDARY_PREFERRED"
    ) -> dict:
        query["target"] = target
        query["date_field"] = "created_year"
        query["date_field_format"] = "%Y"
        _LOGGER.debug(f"[analyze_yearly_cloud_service_stats] query: {query}")
        return self.monthly_stats_model.analyze(**query)

    @cache.cacheable(
        key="inventory:cloud-service-stats:daily:{domain_id}:{query_set_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_cloud_service_stats_with_cache(
        self,
        query: dict,
        query_hash: str,
        domain_id: str,
        query_set_id: str,
        target: str = "SECONDARY_PREFERRED",
    ) -> dict:
        return self.analyze_cloud_service_stats(query, target)

    @cache.cacheable(
        key="inventory:cloud-service-stats:monthly:{domain_id}:{query_set_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_monthly_cloud_service_stats_with_cache(
        self,
        query: dict,
        query_hash: str,
        domain_id: str,
        query_set_id: str,
        target: str = "SECONDARY_PREFERRED",
    ) -> dict:
        return self.analyze_monthly_cloud_service_stats(query, target)

    @cache.cacheable(
        key="inventory:cloud-service-stats:yearly:{domain_id}:{query_set_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_yearly_cloud_service_stats_with_cache(
        self,
        query: dict,
        query_hash: str,
        domain_id: str,
        query_set_id: str,
        target: str = "SECONDARY_PREFERRED",
    ) -> dict:
        return self.analyze_yearly_cloud_service_stats(query, target)

    def analyze_cloud_service_stats_by_granularity(
        self, query: dict, domain_id: str, query_set_id: str
    ) -> dict:
        self._check_date_range(query)
        granularity = query["granularity"]

        # Save query history to speed up data loading
        query_hash = utils.dict_to_hash(query)
        self.create_cloud_service_stats_query_history(
            query, query_hash, domain_id, query_set_id
        )

        if granularity == "DAILY":
            response = self.analyze_cloud_service_stats_with_cache(
                query, query_hash, domain_id, query_set_id
            )
        elif granularity == "MONTHLY":
            response = self.analyze_monthly_cloud_service_stats_with_cache(
                query, query_hash, domain_id, query_set_id
            )
        else:
            response = self.analyze_yearly_cloud_service_stats_with_cache(
                query, query_hash, domain_id, query_set_id
            )

        return response

    @cache.cacheable(
        key="inventory:stats-query-history:{domain_id}:{query_set_id}:{query_hash}",
        expire=600,
    )
    def create_cloud_service_stats_query_history(
        self, query: dict, query_hash: str, domain_id: str, query_set_id: str
    ):
        def _rollback(vo: CloudServiceStatsQueryHistory):
            _LOGGER.info(
                f"[create_cloud_service_stats_query_history._rollback] Delete query history: {query_hash}"
            )
            vo.delete()

        history_model: CloudServiceStatsQueryHistory = self.locator.get_model(
            "CloudServiceStatsQueryHistory"
        )

        history_vos = history_model.filter(query_hash=query_hash, domain_id=domain_id)
        if history_vos.count() == 0:
            history_vo = history_model.create(
                {
                    "query_hash": query_hash,
                    "query_options": copy.deepcopy(query),
                    "query_set_id": query_set_id,
                    "domain_id": domain_id,
                }
            )

            self.transaction.add_rollback(_rollback, history_vo)
        else:
            history_vos[0].update({})

    def _check_date_range(self, query: dict) -> None:
        start_str = query.get("start")
        end_str = query.get("end")
        granularity = query.get("granularity")

        start = self._parse_start_time(start_str, granularity)
        end = self._parse_end_time(end_str, granularity)
        now = datetime.utcnow().date()

        if len(start_str) != len(end_str):
            raise ERROR_INVALID_DATE_RANGE(
                start=start_str,
                end=end_str,
                reason="Start date and end date must be the same format.",
            )

        if start >= end:
            raise ERROR_INVALID_DATE_RANGE(
                start=start_str,
                end=end_str,
                reason="End date must be greater than start date.",
            )

        if granularity == "DAILY":
            if start + relativedelta(months=1) < end:
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="Request up to a maximum of 1 month.",
                )

            if start + relativedelta(months=12) < now.replace(day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For DAILY, you cannot request data older than 1 year.",
                )

        elif granularity == "MONTHLY":
            if start + relativedelta(months=12) < end:
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="Request up to a maximum of 12 months.",
                )

            if start + relativedelta(months=36) < now.replace(day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For MONTHLY, you cannot request data older than 3 years.",
                )
        elif granularity == "YEARLY":
            if start + relativedelta(years=3) < now.replace(month=1, day=1):
                raise ERROR_INVALID_DATE_RANGE(
                    start=start_str,
                    end=end_str,
                    reason="For YEARLY, you cannot request data older than 3 years.",
                )

    def _parse_start_time(self, date_str: str, granularity: str) -> date:
        return self._convert_date_from_string(date_str.strip(), "start", granularity)

    def _parse_end_time(self, date_str: str, granularity: str) -> date:
        end = self._convert_date_from_string(date_str.strip(), "end", granularity)

        if granularity == "YEARLY":
            return end + relativedelta(years=1)
        elif granularity == "MONTHLY":
            return end + relativedelta(months=1)
        else:
            return end + relativedelta(days=1)

    @staticmethod
    def _convert_date_from_string(date_str: str, key: str, granularity: str) -> date:
        if granularity == "YEARLY":
            date_format = "%Y"
            date_type = "YYYY"
        elif granularity == "MONTHLY":
            if len(date_str) == 4:
                date_format = "%Y"
                date_type = "YYYY"
            else:
                date_format = "%Y-%m"
                date_type = "YYYY-MM"
        else:
            if len(date_str) == 4:
                date_format = "%Y"
                date_type = "YYYY"
            elif len(date_str) == 7:
                date_format = "%Y-%m"
                date_type = "YYYY-MM"
            else:
                date_format = "%Y-%m-%d"
                date_type = "YYYY-MM-DD"

        try:
            return datetime.strptime(date_str, date_format).date()
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=date_type)

    @staticmethod
    def _append_status_filter(query: dict) -> dict:
        query_filter = query.get("filter", [])
        query_filter.append({"k": "status", "v": "DONE", "o": "eq"})
        query["filter"] = query_filter
        return query
