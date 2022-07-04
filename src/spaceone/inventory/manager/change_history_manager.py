import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.change_history_model import Record

_LOGGER = logging.getLogger(__name__)


class ChangeHistoryManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.record_model: Record = self.locator.get_model('Record')

    def get_record(self, record_id, domain_id, only=None):
        return self.record_model.get(record_id=record_id, domain_id=domain_id, only=only)

    def filter_records(self, **conditions):
        return self.record_model.filter(**conditions)

    def list_records(self, query={}):
        return self.record_model.query(**query)

    def stat_records(self, query):
        return self.record_model.stat(**query)
