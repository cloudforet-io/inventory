# -*- coding: utf-8 -*-
from spaceone.core.error import *


class ERROR_INVALID_NETWORK(ERROR_BASE):
    _message = 'Network "{cidr}" is invalid format.'


class ERROR_INVALID_CIDR_IN_NETWORK(ERROR_BASE):
    _message = 'CIDR is invalid in network (cidr={cidr})'


class ERROR_EXIST_SUBNET_IN_NETWORK(ERROR_BASE):
    _message = 'Subnet "{subnet_id}" is exist in network ({network_id}).'


class ERROR_DUPLICATE_CIDR(ERROR_BASE):
    _message = 'CIDR was duplicated within zone. (cidr1={cidr1}, cidr2={cidr2})'
