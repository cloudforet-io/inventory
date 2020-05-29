from spaceone.core.error import *


class ERROR_NOT_ALLOW_PINNING_KEYS(ERROR_INVALID_ARGUMENT):
    _message = 'Pinning keys not allowed. (key={key})'


class ERROR_METADATA_DICT_TYPE(ERROR_INVALID_ARGUMENT):
    _message = 'The value of metadata\'s {key} must be a dict type.'


class ERROR_METADATA_LIST_VALUE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = 'The value of metadata\'s {key} must be a list type.'
