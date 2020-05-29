# -*- coding: utf-8 -*-
from spaceone.core.error import *

class ERROR_EXIST_SUBNET_IN_NETWORK_POLICY(ERROR_BASE):
    _message = 'Subnet "{subnet_id}" is exist in network policy ({network_policy_id}).'
