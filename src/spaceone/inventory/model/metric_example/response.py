from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["MetricExampleResponse", "MetricExamplesResponse"]


class MetricExampleResponse(BaseModel):
    example_id: Union[str, None] = None
    name: Union[str, None] = None
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    metric_id: Union[str, None] = None
    namespace_id: Union[str, None] = None
    user_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data["updated_at"])
        return data


class MetricExamplesResponse(BaseModel):
    results: List[MetricExampleResponse] = []
    total_count: int
