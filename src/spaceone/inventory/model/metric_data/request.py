from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "MetricDataSearchQueryRequest",
    "MetricDataAnalyzeQueryRequest",
    "MetricDataStatQueryRequest",
]


class MetricDataSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    metric_id: str
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class MetricDataAnalyzeQueryRequest(BaseModel):
    query: dict
    metric_id: str
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class MetricDataStatQueryRequest(BaseModel):
    query: dict
    metric_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None
