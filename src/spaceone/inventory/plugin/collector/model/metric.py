from typing import Union, Literal
from pydantic import BaseModel

MetricType = Literal["COUNTER", "GAUGE"]


class Metric(BaseModel):
    metric_id: Union[str, None] = None
    name: str
    metric_type: MetricType
    resource_type: str
    query_options: dict
    date_field: Union[str, None] = None
    unit: Union[str, None] = None
    tags: Union[dict, None] = {}
    namespace_id: str
    version: str
