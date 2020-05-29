# -*- coding: utf-8 -*-
from spaceone.core.error import *


class ERROR_INVALID_IP_ADDRESS(ERROR_BASE):
    _message = 'IP address "{ip}" is invalid format.'


class ERROR_ALREADY_USE_IP(ERROR_BASE):
    _message = 'IP "{ip}" was already was in used.'


class ERROR_NOT_ALLOCATED_IP(ERROR_BASE):
    _message = 'IP ({ip}) is not allocated.'


class ERROR_INVALID_IP_STATE(ERROR_BASE):
    _message = 'IP state is invalid for allocate (ip={ip}, state={state})'


class ERROR_NOT_AVAILABLE_IP_IN_SUBNET(ERROR_BASE):
    _message = 'There is no available IP in subnet (subnet_id={subnet_id})'



