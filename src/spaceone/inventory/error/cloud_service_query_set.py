from spaceone.core.error import *


class ERROR_NOT_ALLOWED_QUERY_TYPE(ERROR_INVALID_ARGUMENT):
    _message = 'Managed query set does not allow to update or delete.'


class ERROR_CLOUD_SERVICE_QUERY_SET_STATE(ERROR_INVALID_ARGUMENT):
    _message = 'Query set is not available. (state = {state})'


class ERROR_CLOUD_SERVICE_QUERY_SET_RUN_FAILED(ERROR_UNKNOWN):
    _message = 'Query set run failed. (query_set_id = {query_set_id})'
