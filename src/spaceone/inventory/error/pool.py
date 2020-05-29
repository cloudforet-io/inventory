# -*- coding: utf-8 -*-
from spaceone.core.error import *


class ERROR_NOT_FOUND_USER_IN_POOL(ERROR_BASE):
    _message = 'A user "{user_id}" is not exist in pool ({pool_id}).'


class ERROR_ALREADY_EXIST_USER_IN_POOL(ERROR_BASE):
    _message = 'A user "{user_id}" is already exist in pool ({pool_id}).'
