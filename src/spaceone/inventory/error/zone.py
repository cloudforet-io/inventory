# -*- coding: utf-8 -*-
from spaceone.core.error import *


class ERROR_NOT_FOUND_USER_IN_ZONE(ERROR_BASE):
    _message = 'A user "{user_id}" is not exist in zone ({zone_id}).'


class ERROR_ALREADY_EXIST_USER_IN_ZONE(ERROR_BASE):
    _message = 'A user "{user_id}" is already exist in zone ({zone_id}).'
