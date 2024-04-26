from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "MetricExampleCreateRequest",
    "MetricExampleUpdateRequest",
    "MetricExampleDeleteRequest",
    "MetricExampleGetRequest",
    "MetricExampleSearchQueryRequest",
    "MetricExampleStatQueryRequest",
]


class MetricExampleCreateRequest(BaseModel):
    metric_id: Union[str, None] = None
    name: str
    options: dict
    tags: Union[dict, None] = {}
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class MetricExampleUpdateRequest(BaseModel):
    example_id: str
    name: Union[str, None] = None
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class MetricExampleDeleteRequest(BaseModel):
    example_id: str
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class MetricExampleGetRequest(BaseModel):
    example_id: str
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class MetricExampleSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    example_id: Union[str, None] = None
    name: Union[str, None] = None
    metric_id: Union[str, None] = None
    namespace_id: Union[str, None] = None
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class MetricExampleStatQueryRequest(BaseModel):
    query: dict
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str
