import logging
from typing import Tuple
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.core import utils, cache
from spaceone.inventory.model.metric_data.database import (
    MetricData,
    MonthlyMetricData,
)
from spaceone.inventory.error.metric import (
    ERROR_INVALID_DATE_RANGE,
    ERROR_INVALID_PARAMETER_TYPE,
)

_LOGGER = logging.getLogger(__name__)


class MetricDataManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metric_data_model = MetricData
        self.monthly_metric_data = MonthlyMetricData

    def create_metric_data(self, params: dict) -> MetricData:
        metric_data_vo: MetricData = self.metric_data_model.create(params)
        return metric_data_vo

    def create_monthly_metric_data(self, params: dict) -> MonthlyMetricData:
        monthly_metric_data_vo: MonthlyMetricData = self.monthly_metric_data.create(
            params
        )
        return monthly_metric_data_vo

    def delete_metric_data_by_metric_id(self, metric_id: str, domain_id: str):
        _LOGGER.debug(
            f"[delete_metric_data_by_metric_id] Delete all metric data: {metric_id}"
        )
        metric_data_vos = self.metric_data_model.filter(
            metric_id=metric_id, domain_id=domain_id
        )
        metric_data_vos.delete()

        monthly_metric_data_vos = self.monthly_metric_data.filter(
            metric_id=metric_id, domain_id=domain_id
        )
        monthly_metric_data_vos.delete()

        cache.delete_pattern(f"inventory:metric-data:*:{domain_id}:{metric_id}:*")

    def filter_metric_data(self, **conditions) -> QuerySet:
        return self.metric_data_model.filter(**conditions)

    def filter_monthly_metric_data(self, **conditions) -> QuerySet:
        return self.monthly_metric_data.filter(**conditions)

    def list_metric_data(self, query: dict) -> Tuple[QuerySet, int]:
        query = self._append_status_filter(query)
        return self.metric_data_model.query(**query)

    def list_monthly_metric_data(self, query: dict) -> Tuple[QuerySet, int]:
        query = self._append_status_filter(query)
        return self.monthly_metric_data.query(**query)

    def stat_metric_data(self, query: dict) -> dict:
        query = self._append_status_filter(query)
        return self.metric_data_model.stat(**query)

    def stat_monthly_metric_data(self, query: dict) -> dict:
        query = self._append_status_filter(query)
        return self.monthly_metric_data.stat(**query)

    def analyze_metric_data(
        self, query: dict, target: str = "SECONDARY_PREFERRED"
    ) -> dict:
        query["target"] = target
        query["date_field"] = "created_date"
        query["date_field_format"] = "%Y-%m-%d"
        _LOGGER.debug(f"[analyze_metric_data] query: {query}")
        return self.metric_data_model.analyze(**query)

    def analyze_monthly_metric_data(
        self, query: dict, target: str = "SECONDARY_PREFERRED"
    ) -> dict:
        query["target"] = target
        query["date_field"] = "created_month"
        query["date_field_format"] = "%Y-%m"
        _LOGGER.debug(f"[analyze_monthly_metric_data] query: {query}")
        return self.monthly_metric_data.analyze(**query)

    def analyze_yearly_metric_data(
        self, query: dict, target: str = "SECONDARY_PREFERRED"
    ) -> dict:
        query["target"] = target
        query["date_field"] = "created_year"
        query["date_field_format"] = "%Y"
        _LOGGER.debug(f"[analyze_yearly_metric_data] query: {query}")
        return self.monthly_metric_data.analyze(**query)

    @cache.cacheable(
        key="inventory:metric-data:daily:{domain_id}:{metric_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_metric_data_with_cache(
        self,
        query: dict,
        query_hash: str,
        domain_id: str,
        metric_id: str,
        target: str = "SECONDARY_PREFERRED",
    ) -> dict:
        return self.analyze_metric_data(query, target)

    @cache.cacheable(
        key="inventory:metric-data:monthly:{domain_id}:{metric_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_monthly_metric_data_with_cache(
        self,
        query: dict,
        query_hash: str,
        domain_id: str,
        metric_id: str,
        target: str = "SECONDARY_PREFERRED",
    ) -> dict:
        return self.analyze_monthly_metric_data(query, target)

    @cache.cacheable(
        key="inventory:metric-data:yearly:{domain_id}:{metric_id}:{query_hash}",
        expire=3600 * 24,
    )
    def analyze_yearly_metric_data_with_cache(
        self,
        query: dict,
        query_hash: str,
        domain_id: str,
        metric_id: str,
        target: str = "SECONDARY_PREFERRED",
    ) -> dict:
        return self.analyze_yearly_metric_data(query, target)

    def analyze_metric_data_by_granularity(
        self, query: dict, domain_id: str, metric_id: str
    ) -> dict:
        self._check_date_range(query)
        granularity = query["granularity"]
        query_hash = utils.dict_to_hash(query)

        if granularity == "DAILY":
            response = self.analyze_metric_data_with_cache(
                query, query_hash, domain_id, metric_id
            )
        elif granularity == "MONTHLY":
            response = self.analyze_monthly_metric_data_with_cache(
                query, query_hash, domain_id, metric_id
            )
        else:
            response = self.analyze_yearly_metric_data_with_cache(
                query, query_hash, domain_id, metric_id
            )

        return response

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
