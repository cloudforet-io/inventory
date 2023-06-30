import logging
from datetime import datetime

from spaceone.core import config
from spaceone.core import utils
from spaceone.core.token import get_token
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler

_LOGGER = logging.getLogger(__name__)


class CloudServiceStatsScheduler(HourlyScheduler):

    def __init__(self, queue, interval, minute=':00'):
        super().__init__(queue, interval, minute)
        self.locator = Locator()
        self._init_config()
        self._create_metadata()

    def _init_config(self):
        self._token = get_token('TOKEN')
        self._stats_schedule_hour = config.get_global('STATS_SCHEDULE_HOUR', 16)

    def _create_metadata(self):
        self._metadata = {
            'service': 'inventory',
            'resource': 'CloudServiceQuerySet',
            'verb': 'run_all_query_sets',
            'token': self._token,
        }

    def create_task(self):
        if datetime.utcnow().hour == self._stats_schedule_hour:
            stp = {
                'name': 'cloud_service_stats_schedule',
                'version': 'v1',
                'executionEngine': 'BaseWorker',
                'stages': [{
                    'locator': 'SERVICE',
                    'name': 'CloudServiceQuerySetService',
                    'metadata': self._metadata,
                    'method': 'run_all_query_sets',
                    'params': {
                        'params': {}
                    }
                }]
            }

            print(f'{utils.datetime_to_iso8601(datetime.now())} '
                  f'[INFO] [create_task] run_all_query_sets => START')
            return [stp]
        else:
            print(f'{utils.datetime_to_iso8601(datetime.now())} '
                  f'[INFO] [create_task] run_all_query_sets => SKIP')
            print(f'{utils.datetime_to_iso8601(datetime.now())} [INFO] '
                  f'[create_task] data_source_sync_time: {self._stats_schedule_hour} hour (UTC)')
            return []
