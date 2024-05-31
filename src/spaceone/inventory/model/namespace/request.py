from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "NamespaceCreateRequest",
    "NamespaceUpdateRequest",
    "NamespaceDeleteRequest",
    "NamespaceGetRequest",
    "NamespaceSearchQueryRequest",
    "NamespaceStatQueryRequest",
    "ResourceGroup",
]

Category = Literal["ASSET", "SECURITY", "INFORMATION", "RECOMMENDATION"]
ResourceGroup = Literal["DOMAIN", "WORKSPACE"]


class NamespaceCreateRequest(BaseModel):
    namespace_id: Union[str, None] = None
    name: str
    category: Category
    resource_type: str
    group: Union[str, None] = None
    icon: Union[str, None] = None
    tags: Union[dict, None] = {}
    resource_group: ResourceGroup
    workspace_id: Union[str, None] = None
    domain_id: str


class NamespaceUpdateRequest(BaseModel):
    namespace_id: str
    name: Union[str, None] = None
    icon: Union[str, None] = None
    tags: Union[dict, None] = None
    workspace_id: Union[str, None] = None
    workspace_id: str
    domain_id: str


class NamespaceDeleteRequest(BaseModel):
    namespace_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class NamespaceGetRequest(BaseModel):
    namespace_id: str
    workspace_id: Union[str, list, None] = None
    domain_id: str


class NamespaceSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    namespace_id: Union[str, None] = None
    category: Union[Category, None] = None
    resource_type: Union[str, None] = None
    group: Union[str, None] = None
    is_managed: Union[bool, None] = None
    workspace_id: Union[str, list, None] = None
    domain_id: str


class NamespaceStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, list, None] = None
    domain_id: str
