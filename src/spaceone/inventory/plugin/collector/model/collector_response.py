from typing import List, Union
from enum import Enum
from pydantic import BaseModel
from spaceone.inventory.plugin.collector.model.cloud_service import CloudService
from spaceone.inventory.plugin.collector.model.cloud_service_type import CloudServiceType
from spaceone.inventory.plugin.collector.model.region import Region

__all__ = ['PluginResponse', 'ResourceResponse']


class State(str, Enum):
    success = 'SUCCESS'
    failure = 'FAILURE'


class ResourceType(str, Enum):
    cloud_service = 'inventory.CloudService'
    cloud_service_type = 'inventory.CloudServiceType'
    region = 'inventory.Region'
    error = 'inventory.ErrorResource'


class PluginMetadata(BaseModel):
    supported_resource_type: List[str] = ['inventory.CloudService', 'inventory.CloudServiceType', 'inventory.Region']
    supported_schedules: List[str] = ['hours']
    supported_features: List[str] = ['garbage_collection']
    filter_format: List[str] = []
    options_schema: dict = {}


class PluginResponse(BaseModel):
    metadata: PluginMetadata


class ResourceResponse(BaseModel):
    state: State
    resource_type: ResourceType
    cloud_service_type: CloudServiceType = None
    cloud_service: CloudService = None
    region: Region = None
    match_keys: List[List[str]] = []
    error_message: str = ''

    class Config:
        use_enum_values = True
