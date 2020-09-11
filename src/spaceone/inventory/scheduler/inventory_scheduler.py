# -*- coding: utf-8 -*-
import consul
import datetime
import logging
import time

from spaceone.core import config
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler
from spaceone.core.auth.jwt.jwt_util import JWTUtil

__all__ = ['InventoryHourlyScheduler']

_LOGGER = logging.getLogger(__name__)


def _get_domain_id_from_token(token):
    decoded_token = JWTUtil.unverified_decode(token)
    return decoded_token['did']


WAIT_QUEUE_INITIALIZED = 10     # seconds for waiting queue initilization
INTERVAL = 10
MAX_COUNT = 10


def _validate_token(token):
    if isinstance(token, dict):
        protocol = token['protocol']
        if protocol == 'consul':
            consul_instance = Consul(token['config'])
            value = False
            while value is False:
                uri = token['uri']
                value = consul_instance.patch_token(uri)
                if value:
                    _LOGGER.warn(f'[_validate_token] token: {value[:30]} uri: {uri}')
                    break
                _LOGGER.warn(f'[_validate_token] token is not found ... wait')
                time.sleep(INTERVAL)

            token = value
    return token


class InventoryHourlyScheduler(HourlyScheduler):
    def __init__(self, queue, interval, minute=':00'):
        super().__init__(queue, interval, minute)
        self.count = self._init_count()
        self.locator = Locator()
        self.TOKEN = self._update_token()
        self.domain_id = _get_domain_id_from_token(self.TOKEN)

    def _init_count(self):
        # get current time
        cur = datetime.datetime.now()
        count = {
            'previous': cur,            # Last check_count time
            'index': 0,                # index
            'hour': cur.hour,           # previous hour
            'started_at': 0,            # start time of push_token
            'ended_at': 0               # end time of execution in this tick
            }
        _LOGGER.debug(f'[_init_count] {count}')
        return count

    def _update_token(self):
        token = config.get_global('TOKEN')
        if token == "":
            token = _validate_token(config.get_global('TOKEN_INFO'))
        return token

    def create_task(self):
        # self.check_global_configuration()
        schedules = self.list_schedules()
        result = []
        for schedule in schedules:
            try:
                stp = self._create_job_request(schedule)
                result.append(stp)
            except Exception as e:
                _LOGGER.error(f'[create_task] check schedule {schedule}')

        return result

    def list_schedules(self):
        try:
            ok = self.check_count()
            if ok == False:
                # ERROR LOGGING
                pass
            # Loop all domain, then find scheduled collector
            collector_svc = self.locator.get_service('CollectorService')
            schedule = {'hour': self.count['hour']}
            _LOGGER.debug(f'[push_token] schedule: {schedule}')
            schedule_vos, total = collector_svc.scheduled_collectors({'schedule': schedule})
            _LOGGER.debug(f'[push_token] scheduled count: {total}')
            return schedule_vos
        except Exception as e:
            _LOGGER.error(e)
            return []

    def check_count(self):
        # check current count is correct or not
        cur = datetime.datetime.now()
        hour = cur.hour
        # check
        if (self.count['hour'] + self.config) % 24 != hour:
            if self.count['hour'] == hour:
                _LOGGER.error('[check_count] duplicated call in the same time')
            else:
                _LOGGER.error('[check_count] missing time')

        # This is continuous task
        count = {
            'previous': cur,
            'index': self.count['index'] + 1,
            'hour': hour,
            'started_at': cur
            }
        self.count.update(count)

    def _update_count_ended_at(self):
        cur = datetime.datetime.now()
        self.count['ended_at'] = cur

    def _create_job_request(self, scheduler_vo):
        """ Based on scheduler_vo, create Job Request

        Args:
            scheduler_vo: Scheduler VO
                - scheduler_id
                - name
                - collector: Reference of Collector
                - schedule
                - filter
                - collector_mode
                - created_at
                - last_scheduled_at
                - domain_id
                }

        Returns:
            jobs: SpaceONE Pipeline Template

        Because if collector_info has credential_group_id,
        we have to iterate all credentials in the credential_group
        """
        _LOGGER.debug(f'[_create_job_request] scheduler_vo: {scheduler_vo}')
        plugin_info = scheduler_vo.collector.plugin_info
        _LOGGER.debug(f'plugin_info: {plugin_info}')
        domain_id = scheduler_vo.domain_id
        metadata = {'token': self.TOKEN, 'domain_id': self.domain_id}
        sched_job = {
            'locator': 'SERVICE',
            'name': 'CollectorService',
            'metadata': metadata,
            'method': 'collect',
            'params': {'params': {
                            'collector_id': scheduler_vo.collector.collector_id,
                            # if filter
                            # contact credential
                            'collect_mode': 'ALL',
                            'filter': {},
                            'domain_id': domain_id
                            }
                       }
            }
        stp = {'name': 'inventory_collect_schedule',
               'version': 'v1',
               'executionEngine': 'BaseWorker',
               'stages': [sched_job]}
        _LOGGER.debug(f'[_create_job_request] tasks: {stp}')
        return stp


class Consul:
    def __init__(self, config):
        """
        Args:
          - config: connection parameter

        Example:
            config = {
                    'host': 'consul.example.com',
                    'port': 8500
                }
        """
        self.config = self._validate_config(config)

    def _validate_config(self, config):
        """
        Parameter for Consul
        - host, port=8500, token=None, scheme=http, consistency=default, dc=None, verify=True, cert=None
        """
        options = ['host', 'port', 'token', 'scheme', 'consistency', 'dc', 'verify', 'cert']
        result = {}
        for item in options:
            value = config.get(item, None)
            if value:
                result[item] = value
        return result

    def patch_token(self, key):
        """
        Args:
            key: Query key (ex. /debug/supervisor/TOKEN)

        """
        try:
            conn = consul.Consul(**self.config)
            index, data = conn.kv.get(key)
            return data['Value'].decode('ascii')

        except Exception as e:
            _LOGGER.debug(f'[patch_token] failed: {e}')
            return False
