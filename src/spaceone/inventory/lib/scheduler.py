import datetime
from spaceone.core import config
from spaceone.core.token import _validate_token
from spaceone.core.auth.jwt.jwt_util import JWTUtil


def init_count():
    current_time = datetime.datetime.utcnow()
    return {
        'previous': current_time,  # Last check_count time
        'index': 0,  # index
        'hour': current_time.hour,  # previous hour
        'started_at': 0,  # start time of push_token
        'ended_at': 0  # end time of execution in this tick
    }


def update_token():
    token = config.get_global('TOKEN')
    if token == "":
        token = _validate_token(config.get_global('TOKEN_INFO'))
    return token


def get_domain_id_from_token(token):
    decoded_token = JWTUtil.unverified_decode(token)
    return decoded_token['did']


def get_interval_value(schedule_vo):
    return schedule_vo.schedule.interval
