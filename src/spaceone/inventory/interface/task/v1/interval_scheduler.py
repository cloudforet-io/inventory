"""
DEPRECATED
"""
import logging
import time
import schedule
import json
from jsonschema import validate
from spaceone.core import queue
from spaceone.core.locator import Locator
from spaceone.core.scheduler import IntervalScheduler
from spaceone.core.scheduler.task_schema import SPACEONE_TASK_SCHEMA
from spaceone.inventory.lib.scheduler import init_count, update_token, get_domain_id_from_token, get_interval_value
from spaceone.inventory.service.collector_service import CollectorService

_LOGGER = logging.getLogger(__name__)


"""
IntervalExecutor is a sync engine, for IntervalScheduler
Sync DB in every N seconds.

Load all interval schedule, and create IntervalScheduler
"""
class InventoryIntervalScheduler(IntervalScheduler):

    def __init__(self, queue, interval):
        super().__init__(queue, interval)
        self.count = init_count()
        self.locator = Locator()
        self.TOKEN = update_token()
        self.domain_id = get_domain_id_from_token(self.TOKEN)
        self.schedule_info = {}
        self.idx = 0

    def run(self):
        # Every specific interval, check schedule
        schedule.every(self.config).seconds.do(self._check_interval)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def push_task(self, schedule_id, interval_info):
        task = self._create_job_request(schedule_id, interval_info)
        _LOGGER.debug(f'[push_task] {task["name"]}')
        try:
            validate(task, schema=SPACEONE_TASK_SCHEMA)
            queue.put(self.queue, json.dumps(task))
        except Exception as e:
            print(e)
            _LOGGER.debug(f'[push_task] Task schema: {task}, {e}')

    def _check_interval(self):
        # patch schedule and sync
        # create new interval Schedule per schedule_id
        # interval_schedules = {schedule-zzz: {interval: 20, domain_id: domain-1234, collector_id: collector-5678},
        #                       schedule_yyy: {interval: 30, domain_id: domain-2222, collector_id: collector-3333}}

        interval_schedules = self._get_interval_schedules()
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

            if exist is False:
                _LOGGER.debug(f'[_check_interval] remove job: {job}')
                schedule.default_scheduler.cancel_job(job)

    def _get_interval_schedules(self):
        found_schedules = {}

        for schedule_vo in self._list_schedules():
            try:
                interval_value = get_interval_value(schedule_vo)
                found_schedules.update({
                    schedule_vo.schedule_id: {
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
            collector_svc: CollectorService = self.locator.get_service(CollectorService)
            schedule_vos, total = collector_svc.scheduled_collectors({'schedule': {'interval': None}})
            return schedule_vos
        except Exception as e:
            _LOGGER.error(e)
            return []

    def _create_job_request(self, schedule_id, interval_info):
        _LOGGER.debug(f'[_create_job_request] {interval_info}')
        domain_id = interval_info['domain_id']

        metadata = {
            'token': self.TOKEN,
            'service': 'inventory',
            'resource': 'Collector',
            'verb': 'collect',
            'domain_id': self.domain_id
        }

        schedule_job = {
            'locator': 'SERVICE',
            'name': 'CollectorService',
            'metadata': metadata,
            'method': 'collect',
            'params': {
                'params': {
                    'collector_id': interval_info['collector_id'],
                    'collect_mode': 'ALL',
                    'filter': {},
                    'domain_id': domain_id
                }
            }
        }

        return {
            'name': 'inventory_collect_by_interval_schedule',
            'version': 'v1',
            'executionEngine': 'BaseWorker',
            'stages': [schedule_job]
        }

    # @staticmethod
    # def _init_count():
    #     # get current time
    #     cur = datetime.datetime.utcnow()
    #     count = {
    #         'previous': cur,            # Last check_count time
    #         'index': 0,                # index
    #         'hour': cur.hour,           # previous hour
    #         'started_at': 0,            # start time of push_token
    #         'ended_at': 0               # end time of execution in this tick
    #         }
    #     _LOGGER.debug(f'[_init_count] {count}')
    #     return count
    #
    # @staticmethod
    # def _update_token():
    #     token = config.get_global('TOKEN')
    #     if token == "":
    #         token = _validate_token(config.get_global('TOKEN_INFO'))
    #     return token
    #
    # @staticmethod
    # def _get_domain_id_from_token(token):
    #     decoded_token = JWTUtil.unverified_decode(token)
    #     return decoded_token['did']
    #
    # @staticmethod
    # def _get_interval_value(schedule_vo):
    #     # get interval vaule from schedule vo
    #     return schedule_vo.schedule.interval


# class Consul:
#     def __init__(self, config):
#         """
#         Args:
#           - config: connection parameter
#
#         Example:
#             config = {
#                     'host': 'consul.example.com',
#                     'port': 8500
#                 }
#         """
#         self.config = self._validate_config(config)
#
#     def _validate_config(self, config):
#         """
#         Parameter for Consul
#         - host, port=8500, token=None, scheme=http, consistency=default, dc=None, verify=True, cert=None
#         """
#         options = ['host', 'port', 'token', 'scheme', 'consistency', 'dc', 'verify', 'cert']
#         result = {}
#         for item in options:
#             value = config.get(item, None)
#             if value:
#                 result[item] = value
#         return result
#
#     def patch_token(self, key):
#         """
#         Args:
#             key: Query key (ex. /debug/supervisor/TOKEN)
#
#         """
#         try:
#             conn = consul.Consul(**self.config)
#             index, data = conn.kv.get(key)
#             return data['Value'].decode('ascii')
#
#         except Exception as e:
#             _LOGGER.debug(f'[patch_token] failed: {e}')
#             return False

