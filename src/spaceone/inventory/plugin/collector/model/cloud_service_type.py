from typing import List
from pydantic import BaseModel


class CloudServiceType(BaseModel):
    name: str
    group: str
    provider: str
    is_primary: bool = False
    is_major: bool = False
    metadata: dict = None
    json_metadata: str = None
    service_code: str = None
    tags: dict = {}
    labels: List[str] = []
