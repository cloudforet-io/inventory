# -*- coding: utf-8 -*-
import consul
import datetime
import logging
import time

from spaceone.core import config
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler
from spaceone.core.auth.jwt.jwt_util import JWTUtil

__all__ = ['CleanupScheduler']

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


class CleanupScheduler(HourlyScheduler):
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
        domains = self.list_domains()
        result = []
        for domain in domains:
            stp = self._create_job_request(domain)
            result.append(stp)
        return result

    def list_domains(self):
        try:
            ok = self.check_count()
            if ok == False:
                # ERROR LOGGING
                pass
            # Loop all domain, then find scheduled collector
            metadata = {'token': self.TOKEN, 'domain_id': self.domain_id}
            cleanup_svc = self.locator.get_service('CleanupService', metadata)
            params = {}
            resp = cleanup_svc.list_domains(params)
            _LOGGER.debug(f'[list_domain] num of domains: {resp["total_count"]}')
            return resp['results']
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

    def _create_job_request(self, domain):
        """ Based on domain, create Job Request

        Returns:
            jobs: SpaceONE Pipeline Template
        """
        _LOGGER.debug(f'[_create_job_request] domain: {domain}')
        metadata = {'token': self.TOKEN, 'domain_id': self.domain_id}
        sched_job = {
            'locator': 'SERVICE',
            'name': 'CleanupService',
            'metadata': metadata,
            'method': 'update_collection_state',
            'params': {'params': {
                            'options': {},
                            'domain_id': domain['domain_id']
                            }
                       }
            }

        update_job_state = {
            'locator': 'SERVICE',
            'name': 'CleanupService',
            'metadata': metadata,
            'method': 'update_job_state',
            'params': {'params': {
                            'options': {},
                            'domain_id': domain['domain_id']
                            }
                       }
            }

        delete_resources = {
            'locator': 'SERVICE',
            'name': 'CleanupService',
            'metadata': metadata,
            'method': 'delete_resources',
            'params': {'params': {
                            'options': {},
                            'domain_id': domain['domain_id']
                            }
                       }
        }

        stp = {'name': 'inventory_cleanup_schedule',
               'version': 'v1',
               'executionEngine': 'BaseWorker',
               'stages': [sched_job, update_job_state, delete_resources]}

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
