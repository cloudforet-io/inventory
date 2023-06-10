import datetime
import logging
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler
from spaceone.inventory.lib.scheduler import init_count, update_token, get_domain_id_from_token
from spaceone.inventory.service.collector_service import CollectorService


__all__ = ['InventoryHourlyScheduler']

_LOGGER = logging.getLogger(__name__)


class InventoryHourlyScheduler(HourlyScheduler):
    def __init__(self, queue, interval, minute=':00'):
        super().__init__(queue, interval, minute)
        self.count = init_count()
        self.locator = Locator()
        self.TOKEN = update_token()
        self.domain_id = get_domain_id_from_token(self.TOKEN)

    def create_task(self):
        return [self._create_job_request(collector_vo) for collector_vo in self.list_schedule_collectors()]

    def list_schedule_collectors(self):
        try:
            self.check_count()
            collector_svc: CollectorService = self.locator.get_service(CollectorService)
            collector_vos, total = collector_svc.scheduled_collectors({'schedule': {'hour': self.count['hour']}})
            _LOGGER.debug(f'[push_token] scheduled collectors count: {total}')
            return collector_vos
        except Exception as e:
            _LOGGER.error(e)
            return []

    def check_count(self):
        current_time = datetime.datetime.utcnow()
        hour = current_time.hour

        if (self.count['hour'] + self.config) % 24 != hour:
            if self.count['hour'] == hour:
                _LOGGER.error('[check_count] duplicated call in the same time')
            else:
                _LOGGER.error('[check_count] missing time')

        self.count.update({
            'previous': current_time,
            'index': self.count['index'] + 1,
            'hour': hour,
            'started_at': current_time
        })

    def _update_count_ended_at(self):
        cur = datetime.datetime.utcnow()
        self.count['ended_at'] = cur

    def _create_job_request(self, collector_vo):
        schedule_job = {
            'locator': 'SERVICE',
            'name': 'CollectorService',
            'metadata': self._get_metadata(),
            'method': 'collect',
            'params': {
                'params': {
                    'collector_id': collector_vo.collector_id,
                    'collect_mode': 'ALL',
                    'filter': {},
                    'domain_id': collector_vo.domain_id
                }
            }
        }

        return {
            'name': 'inventory_collect_schedule',
            'version': 'v1',
            'executionEngine': 'BaseWorker',
            'stages': [schedule_job]
        }

    def _get_metadata(self):
        return {
            'token': self.TOKEN,
            'service': 'inventory',
            'resource': 'Collector',
            'verb': 'collect',
            'domain_id': self.domain_id
        }
