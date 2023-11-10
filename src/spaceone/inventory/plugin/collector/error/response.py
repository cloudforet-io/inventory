from spaceone.core.error import *


class ERROR_REQUIRED_FIELDS_MISSING(ERROR_INVALID_ARGUMENT):
    _message = 'Must contain one of "cloud_service_type", "cloud_service", or "region". (current fields={fields})'
