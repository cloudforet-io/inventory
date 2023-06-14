import logging
from datetime import datetime
from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.inventory.error import *
from spaceone.inventory.model.collector_model import Collector


__ALL__ = ['CollectorManager']

_LOGGER = logging.getLogger(__name__)


class CollectorManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collector_model: Collector = self.locator.get_model('Collector')

    def create_collector(self, params):
        """
        Args: params
          - name
          - plugin_info
          - schedule
          - state
          - tags
          - domain_id
        """
        def _rollback(collector_vo):
            _LOGGER.info(f'[ROLLBACK] Delete collector : {collector_vo.name} ({collector_vo.collector_id})')
            collector_vo.delete()

        collector_vo: Collector = self.collector_model.create(params)
        self.transaction.add_rollback(_rollback, collector_vo)
        return collector_vo

    def delete_collector(self, collector_id, domain_id):
        collector_vo = self.collector_model.get(collector_id=collector_id, domain_id=domain_id)
        collector_vo.delete()

    def get_collector(self, collector_id, domain_id, only=None):
        return self.collector_model.get(collector_id=collector_id, domain_id=domain_id, only=only)

    def enable_collector(self, collector_id, domain_id):
        collector_vo: Collector = self.collector_model.get(collector_id=collector_id, domain_id=domain_id)
        return collector_vo.update({'state': 'ENABLED'})

    def disable_collector(self, collector_id, domain_id, plugin_init=True):
        collector_vo: Collector = self.collector_model.get(collector_id=collector_id, domain_id=domain_id)
        return collector_vo.update({'state': 'DISABLED'})

    def list_collectors(self, query):
        return self.collector_model.query(**query)

    def stat_collectors(self, query):
        return self.collector_model.stat(**query)

    def update_last_collected_time(self, collector_vo):
        params = {'last_collected_at': datetime.utcnow()}
        self.update_collector_by_vo(collector_vo, params)

    @staticmethod
    def update_collector_by_vo(collector_vo, params):
        return collector_vo.update(params)

    @staticmethod
    def get_queue_name(name='collect_queue'):
        try:
            return config.get_global(name)
        except Exception as e:
            _LOGGER.warning(f'[_get_queue_name] name: {name} is not configured')
            return None

