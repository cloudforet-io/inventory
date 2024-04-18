from spaceone.core.error import *


class ERROR_NOT_SUPPORT_RESOURCE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = "Not support resource type. (resource_type = {resource_type})"


class ERROR_INVALID_DATE_RANGE(ERROR_INVALID_ARGUMENT):
    _message = "{reason}"


class ERROR_METRIC_QUERY_RUN_FAILED(ERROR_BASE):
    _message = "Metric query run failed. (metric_id = {metric_id})"


class ERROR_WRONG_QUERY_OPTIONS(ERROR_INVALID_ARGUMENT):
    _message = "Wrong query options. (query_options = {query_options})"
