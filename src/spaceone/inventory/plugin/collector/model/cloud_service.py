from typing import List, Optional
from typing_extensions import TypedDict
from pydantic import BaseModel, IPvAnyAddress

__all__ = ["CloudService", "Reference"]


class Reference(TypedDict, total=False):
    resource_id: str
    external_link: str


class CloudService(BaseModel):
    name: str = None
    cloud_service_type: str
    cloud_service_group: str
    provider: str
    ip_addresses: Optional[List[str]] = []
    account: str = None
    instance_type: str = None
    instance_size: float = 0
    launched_at: str = None
    region_code: str = None
    data: dict
    metadata: dict
    reference: Reference = None
    tags: dict = {}
