# -*- coding: utf-8 -*-
from spaceone.core.error import *


class ERROR_INVALID_VLAN(ERROR_BASE):
    _message = 'vlan ({vlan}) is not valid.'


class ERROR_INVALID_IP_IN_CIDR(ERROR_BASE):
    _message = 'IP is invalid in CIDR (ip={ip}, cidr={cidr})'


class ERROR_INVALID_IP_RANGE(ERROR_BASE):
    _message = 'IP Range is invalid (start_ip={start}, end_ip={end})'
