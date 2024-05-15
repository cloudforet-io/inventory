from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils
from spaceone.inventory.model.metric.request import MetricType

__all__ = ["MetricResponse", "MetricsResponse"]


class MetricResponse(BaseModel):
    metric_id: Union[str, None] = None
    name: Union[str, None] = None
    metric_type: Union[MetricType, None] = None
    resource_type: Union[str, None] = None
    query_options: Union[dict, None] = None
    date_field: Union[str, None] = None
    unit: Union[str, None] = None
    tags: Union[dict, None] = None
    labels_info: Union[List[dict], None] = None
    is_managed: Union[bool, None] = None
    namespace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data["updated_at"])
        return data


class MetricsResponse(BaseModel):
    results: List[MetricResponse] = []
    total_count: int
