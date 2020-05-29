# -*- coding: utf-8 -*-
from spaceone.core.error import *

class ERROR_EXIST_SUBNET_IN_NETWORK_TYPE(ERROR_BASE):
    _message = 'Subnet "{subnet_id}" is exist in network type ({network_type_id}).'
