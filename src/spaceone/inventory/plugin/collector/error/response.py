from spaceone.core.error import *


class ERROR_INVAILD_INPUT_FIELD(ERROR_INVALID_ARGUMENT):
    _message = 'Must contain one of "cloud_service_type", "cloud_service", or "region". (current fields={fields})'


class ERROR_NOT_MATCH_RESOURCE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = 'resource_type and resource do not match. (resource_type={resource_type}, resource={resource})'
