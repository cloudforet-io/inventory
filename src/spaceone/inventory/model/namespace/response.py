from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["NamespaceResponse", "NamespacesResponse"]


class NamespaceResponse(BaseModel):
    namespace_id: Union[str, None] = None
    name: Union[str, None] = None
    category: Union[str, None] = None
    provider: Union[str, None] = None
    icon: Union[str, None] = None
    tags: Union[dict, None] = None
    is_managed: Union[bool, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data["updated_at"])
        return data


class NamespacesResponse(BaseModel):
    results: List[NamespaceResponse] = []
    total_count: int
