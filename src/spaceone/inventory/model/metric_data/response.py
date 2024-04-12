from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["MetricDataResponse", "MetricDatasResponse"]


class MetricDataResponse(BaseModel):
    metric_id: Union[str, None] = None
    value: Union[float, None] = None
    unit: Union[str, None] = None
    labels: Union[dict, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    created_year: Union[str, None] = None
    created_month: Union[str, None] = None
    created_date: Union[str, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        return data


class MetricDatasResponse(BaseModel):
    results: List[MetricDataResponse] = []
    total_count: int
