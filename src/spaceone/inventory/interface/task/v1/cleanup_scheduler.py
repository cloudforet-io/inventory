import copy
import datetime
import logging
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler
from spaceone.inventory.service.cleanup_service import CleanupService
from spaceone.inventory.lib.scheduler import init_count, update_token, get_domain_id_from_token


__all__ = ['CleanupScheduler']

_LOGGER = logging.getLogger(__name__)


INTERVAL = 10


class CleanupScheduler(HourlyScheduler):

    def __init__(self, queue, interval, minute=':00'):
        super().__init__(queue, interval, minute)
        self.count = init_count()
        self.locator = Locator()
        self.TOKEN = update_token()
        self.domain_id = get_domain_id_from_token(self.TOKEN)

    def create_task(self):
        return [self._create_job_request(domain) for domain in self.list_domains()]

    def list_domains(self):
        try:
            if self.check_count() is False:
                # ERROR LOGGING
                pass

            # Loop all domain, then find scheduled collector
            metadata = {
                'token': self.TOKEN,
                'service': 'inventory',
                'resource': 'Cleanup',
                'verb': 'list_domains',
                'domain_id': self.domain_id
            }

            cleanup_svc: CleanupService = self.locator.get_service(CleanupService, metadata)
            response = cleanup_svc.list_domains({})
            return response.get('results', [])

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
        current_time = datetime.datetime.utcnow()
        self.count['ended_at'] = current_time

    def _create_job_request(self, domain):
        """ Based on domain, create Job Request
        Returns:
            jobs: SpaceONE Pipeline Template
        """
        # _LOGGER.debug(f'[_create_job_request] domain: {domain}')

        metadata = {
            'token': self.TOKEN,
            'service': 'inventory',
            'resource': 'Cleanup',
            'verb': 'update_job_state',
            'domain_id': self.domain_id
        }

        update_job_state = {
            'locator': 'SERVICE',
            'name': 'CleanupService',
            'metadata': copy.deepcopy(metadata),
            'method': 'update_job_state',
            'params': {
                'params': {
                    'options': {},
                    'domain_id': domain['domain_id']
                }
            }
        }

        metadata['verb'] = 'delete_resources'
        delete_resources = {
            'locator': 'SERVICE',
            'name': 'CleanupService',
            'metadata': copy.deepcopy(metadata),
            'method': 'delete_resources',
            'params': {
                'params': {
                    'options': {},
                    'domain_id': domain['domain_id']
                }
            }
        }

        metadata['verb'] = 'terminate_jobs'
        terminate_jobs = {
            'locator': 'SERVICE',
            'name': 'CleanupService',
            'metadata': copy.deepcopy(metadata),
            'method': 'terminate_jobs',
            'params': {
                'params': {
                    'options': {},
                    'domain_id': domain['domain_id']
                }
            }
        }

        metadata['verb'] = 'terminate_resources'
        terminate_resources = {
            'locator': 'SERVICE',
            'name': 'CleanupService',
            'metadata': copy.deepcopy(metadata),
            'method': 'terminate_resources',
            'params': {
                'params': {
                    'options': {
                    },
                    'domain_id': domain['domain_id']
                }
            }
        }

        stp = {
            'name': 'inventory_cleanup_schedule',
            'version': 'v1',
            'executionEngine': 'BaseWorker',
            'stages': [update_job_state, delete_resources, terminate_jobs, terminate_resources]
        }

        _LOGGER.debug(f'[_create_job_request] tasks: inventory_cleanup_schedule')
        return stp
