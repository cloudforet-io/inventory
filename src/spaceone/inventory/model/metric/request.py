from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "MetricCreateRequest",
    "MetricUpdateRequest",
    "MetricDeleteRequest",
    "MetricRunRequest",
    "MetricTestRequest",
    "MetricGetRequest",
    "MetricSearchQueryRequest",
    "MetricStatQueryRequest",
]

MetricType = Literal["COUNTER", "GAUGE"]


class MetricCreateRequest(BaseModel):
    metric_id: Union[str, None] = None
    name: str
    metric_type: MetricType
    resource_type: str
    query_options: dict
    date_field: Union[str, None] = None
    unit: Union[str, None] = None
    tags: Union[dict, None] = {}
    namespace_id: str
    workspace_id: str
    domain_id: str


class MetricUpdateRequest(BaseModel):
    metric_id: str
    name: Union[str, None] = None
    query_options: Union[dict, None] = None
    date_field: Union[str, None] = None
    unit: Union[str, None] = None
    tags: Union[dict, None] = None
    workspace_id: str
    domain_id: str


class MetricDeleteRequest(BaseModel):
    metric_id: str
    workspace_id: str
    domain_id: str


class MetricRunRequest(BaseModel):
    metric_id: str
    workspace_id: str
    domain_id: str


class MetricTestRequest(BaseModel):
    metric_id: str
    query_options: Union[dict, None] = None
    workspace_id: str
    domain_id: str


class MetricGetRequest(BaseModel):
    metric_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class MetricSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    metric_id: Union[str, None] = None
    metric_type: Union[MetricType, None] = None
    resource_type: Union[str, None] = None
    is_managed: Union[bool, None] = None
    namespace_id: Union[str, None] = None
    workspace_id: Union[list, None] = None
    domain_id: str


class MetricStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[list, None] = None
    domain_id: str
