from spaceone.core.error import *


class ERROR_NOT_ALLOWED_TO_UPDATE_RULE(ERROR_INVALID_ARGUMENT):
    _message = 'If rule_type is MANAGED, it cannot be updated.'


class ERROR_NOT_ALLOWED_TO_CHANGE_ORDER(ERROR_INVALID_ARGUMENT):
    _message = 'If rule_type is MANAGED, it cannot be changed.'


class ERROR_NOT_ALLOWED_TO_DELETE_RULE(ERROR_INVALID_ARGUMENT):
    _message = 'If rule_type is MANAGED, it cannot be deleted.'
