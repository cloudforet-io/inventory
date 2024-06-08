from spaceone.core.error import *


class ERROR_NOT_SUPPORTED_RESOURCE_TYPE(ERROR_UNKNOWN):
    _message = "The resource type is not supported. (resource_type={resource_type})"


class ERROR_REQUIRED_PARAMETER_FOR_RESOURCE_TYPE(ERROR_UNKNOWN):
    _message = "The required parameter is not given for the resource type. (resource_type={resource_type}, key={key})"
