from typing import Union, List
from pydantic import BaseModel


__all__ = ["MetricDataResponse", "MetricDatasResponse"]


class MetricDataResponse(BaseModel):
    metric_id: Union[str, None] = None
    value: Union[float, None] = None
    unit: Union[str, None] = None
    labels: Union[dict, None] = None
    namespace_id: Union[str, None] = None
    service_account_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_year: Union[str, None] = None
    created_month: Union[str, None] = None
    created_date: Union[str, None] = None


class MetricDatasResponse(BaseModel):
    results: List[MetricDataResponse] = []
    total_count: int
