from spaceone.core.error import *


class ERROR_NO_DATA_TO_EXPORT(ERROR_INVALID_ARGUMENT):
    _message = 'No data to export!'


class ERROR_NOT_SUPPORT_FILE_FORMAT(ERROR_INVALID_ARGUMENT):
    _message = 'Not support file format! (file_format = {file_format})'
