# -*- coding: utf-8 -*-
from spaceone.core.error import *


class ERROR_NOT_FOUND_USER_IN_REGION(ERROR_BASE):
    _message = 'A user "{user_id}" is not exist in region ({region_id}).'


class ERROR_ALREADY_EXIST_USER_IN_REGION(ERROR_BASE):
    _message = 'A user "{user_id}" is already exist in region ({region_id}).'
