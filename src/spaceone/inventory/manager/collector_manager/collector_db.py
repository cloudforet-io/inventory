import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.collector_model import Collector

__ALL__ = ['CollectorDB']

_LOGGER = logging.getLogger(__name__)


class CollectorDB(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collector_model: Collector = self.locator.get_model('Collector')

    def create_collector(self, params):
        def _rollback(collector_vo):
            _LOGGER.info(f'[ROLLBACK] Delete collector : {collector_vo.name} ({collector_vo.collector_id})')
            collector_vo.delete()

        collector_vo: Collector = self.collector_model.create(params)
        self.transaction.add_rollback(_rollback, collector_vo)
        return collector_vo

    def delete_collector(self, collector_id, domain_id):
        collector_vo:Collector = self.collector_model.get(collector_id=collector_id, domain_id=domain_id)
        collector_vo.delete()

    def get_collector(self, collector_id, domain_id, only=None):
        collector_vo:Collector = self.collector_model.get(collector_id=collector_id, domain_id=domain_id, only=only)
        return collector_vo

    def enable_collector(self, collector_id, domain_id):
        collector_vo:Collector = self.collector_model.get(collector_id=collector_id, domain_id=domain_id)
        return collector_vo.update({'state':'ENABLED'})

    def disable_collector(self, collector_id, domain_id):
        collector_vo:Collector = self.collector_model.get(collector_id=collector_id, domain_id=domain_id)
        return collector_vo.update({'state':'DISABLED'})

    def list_collectors(self, query):
        return self.collector_model.query(**query)

    def stat_collectors(self, query):
        return self.collector_model.stat(**query)
