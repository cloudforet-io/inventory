from typing import List
from enum import Enum
from pydantic import BaseModel

__all__ = ['PluginResponse', 'ResourceResponse']


class State(str, Enum):
    success = 'SUCCESS'
    failure = 'FAILURE'


class ResourceType(str, Enum):
    cloud_service = 'inventory.CloudService'
    cloud_service_type = 'inventory.CloudServiceType'
    region = 'inventory.Region'
    error = 'inventory.ErrorResource'


class PluginResponse(BaseModel):
    metadata: dict


class ResourceResponse(BaseModel):
    state: State
    resource_type: ResourceType
    resource_data: dict = {}
    match_keys: List[List[str]] = []
    error_message: str = ''

    class Config:
        use_enum_values = True
