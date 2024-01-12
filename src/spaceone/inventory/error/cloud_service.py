from spaceone.core.error import *


class ERROR_RESOURCE_ALREADY_DELETED(ERROR_INVALID_ARGUMENT):
    _message = "{resource_type} has already been deleted. ({resource_id})"
