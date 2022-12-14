from spaceone.core.error import *


class ERROR_INVALID_PRIMARY_IP_ADDRESS(ERROR_INVALID_ARGUMENT):
    _message = 'Primary IP address does not exist in NICs.'


class ERROR_RESOURCE_ALREADY_DELETED(ERROR_INVALID_ARGUMENT):
    _message = '{resource_type} has already been deleted. ({resource_id})'
