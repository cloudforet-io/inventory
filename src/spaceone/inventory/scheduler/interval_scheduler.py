# -*- coding: utf-8 -*-
import consul
import datetime
import logging
import time
import schedule
import json

from jsonschema import validate

from spaceone.core import queue
from spaceone.core import config
from spaceone.core.locator import Locator
from spaceone.core.scheduler import IntervalScheduler
from spaceone.core.auth.jwt.jwt_util import JWTUtil
from spaceone.core.scheduler.task_schema import SPACEONE_TASK_SCHEMA

__all__ = ['IntervalExecutor']

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

"""
IntervalExecutor is a sync engine, for IntervalScheduler
Sync DB in every N seconds.

Load all interval schedule, and create IntervalScheduler
"""
class InventoryIntervalScheduler(IntervalScheduler):
    def __init__(self, queue, interval):
        super().__init__(queue, interval)
        self.count = self._init_count()
        self.locator = Locator()
        self.TOKEN = self._update_token()
        self.domain_id = _get_domain_id_from_token(self.TOKEN)
        self.schedule_info = {}
        self.idx = 0

    def run(self):
        # Every specific interval, check schedule
        schedule.every(self.config).seconds.do(self._check_interval)
        while True:
            schedule.run_pending()
            time.sleep(1)

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

    def push_task(self, schedule_id, interval_info):
        # schedule_id: schedule_2222
        # interval_info: {integval: 30, domain_id: dom-2222, collector_id: collector-3333}
        task = self._create_job_request(schedule_id, interval_info)
        _LOGGER.debug(f'[push_task] {task["name"]}')
        try:
            validate(task, schema=SPACEONE_TASK_SCHEMA)
            json_task = json.dumps(task)
            _LOGGER.debug(f'[push_task] Task schema: {task}')
            queue.put(self.queue, json_task)
        except Exception as e:
            print(e)
            _LOGGER.debug(f'[push_task] Task schema: {task}, {e}')

    def _check_interval(self):
        """ Check interval schedule
        Then run schedule
        """
        # patch schedule and sync
        # create new interval Schedule per schedule_id
        #interval_schedules = {schedule-1234: {interval: 20, domain_id: dom-1234, collector_id: collector-5678},
        #                      schedule_2222: {integval: 30, domain_id: dom-2222, collector_id: collector-3333}
        #                       }
        interval_schedules = self._get_interval_schedules()
        # Update Scheule based on DB
        schedule_ids = []
        for schedule_id, interval_info in interval_schedules.items():
            schedule_ids.append(schedule_id)
            try:
                interval_value = interval_info['interval']
                # Create New scheduler
                if schedule_id not in self.schedule_info:
                    _LOGGER.debug(f'[_check_interval] create {schedule_id}, at every {interval_value}')
                    self.schedule_info[schedule_id] = interval_info
                    job = schedule.every(interval_value).seconds.do(self.push_task, schedule_id, interval_info)
                    # Add tag for each job
                    job.tag(schedule_id)
                # Sync previous scheduler
                else:
                    previous_interval_info = self.schedule_info[schedule_id]
                    previous_interval = previous_interval_info['interval']
                    if interval_value != previous_interval:
                        _LOGGER.debug(f'[_check_interval] modify {schedule_id} interval {previous_interval} to {interval_value}')
                        # delete job and re-create
                        schedule.default_scheduler.clear(tag=schedule_id)
                        job = schedule.every(interval_value).seconds.do(self.push_task, schedule_id, interval_info)
                        job.tag(schedule_id)
                        # Update self.schedule_info
                        self.schedule_info[schedule_id] = interval_info
                    else:
                        _LOGGER.debug(f'[_check_interval] continue {schedule_id}, at every {previous_interval}')
            except Exception as e:
                _LOGGER.error(f'[_check_interval] contact to developer {e}')
        # Delete garbage
        _LOGGER.debug(f'[_check_interval] gargabe collector: {len(schedule.default_scheduler.jobs) - 1}')
        for job in schedule.default_scheduler.jobs:
            if job.tags == set():
                continue
            exist = False
            for schedule_id in schedule_ids:
                if schedule_id in job.tags:
                    print(f'exist: {schedule_id}')
                    exist = True
                    break

            if exist == False:
                # This Job is gargage
                _LOGGER.debug(f'[_check_interval] remove job: {job}')
                schedule.default_scheduler.cancel_job(job)

    def _get_interval_schedules(self):
        """ Find all interval schedules from inventory.Collector.schedule
        """
        schedule_vos = self._list_schedules()
        found_schedules = {}
        for schedule_vo in schedule_vos:
            try:
                interval_value = _get_interval_value(schedule_vo)
                found_schedules.update({schedule_vo.schedule_id:
                                            {
                                            'interval': interval_value,
                                            'domain_id': schedule_vo.domain_id,
                                            'collector_id': schedule_vo.collector.collector_id
                                            }
                                        })
            except Exception as e:
                _LOGGER.error(f'[_get_interval_schedules] {e}')
        _LOGGER.debug(f'[_get_interval_schedules] found: {found_schedules}')
        return found_schedules

    def _list_schedules(self):
        try:
            # Loop all domain, then find scheduled collector
            collector_svc = self.locator.get_service('CollectorService')
            schedule = {'interval': None}
            schedule_vos, total = collector_svc.scheduled_collectors({'schedule': schedule})
            return schedule_vos
        except Exception as e:
            _LOGGER.error(e)
            return []

    def _create_job_request(self, schedule_id, interval_info):
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

        """
        _LOGGER.debug(f'[_create_job_request] {interval_info}')
        domain_id = interval_info['domain_id']
        metadata = {'token': self.TOKEN, 'domain_id': self.domain_id}
        sched_job = {
            'locator': 'SERVICE',
            'name': 'CollectorService',
            'metadata': metadata,
            'method': 'collect',
            'params': {'params': {
                            'collector_id': interval_info['collector_id'],
                            # if filter
                            # contact credential
                            'collect_mode': 'ALL',
                            'filter': {},
                            'domain_id': domain_id
                            }
                       }
            }
        stp = {'name': 'inventory_collect_by_interval_schedule',
               'version': 'v1',
               'executionEngine': 'BaseWorker',
               'stages': [sched_job]}
        #_LOGGER.debug(f'[_create_job_request] tasks: {stp}')
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

def _get_interval_value(schedule_vo):
    # get interval vaule from schedule vo
    return schedule_vo.schedule.interval
