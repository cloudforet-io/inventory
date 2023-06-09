from spaceone.core.error import *


class ERROR_NOT_ALLOWED_QUERY_TYPE(ERROR_BASE):
    _message = 'Managed query set does not allow to update or delete.'
