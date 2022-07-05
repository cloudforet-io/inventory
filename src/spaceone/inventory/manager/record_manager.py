import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.record_model import Record

_LOGGER = logging.getLogger(__name__)


class RecordManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.record_model: Record = self.locator.get_model('Record')

    def create_record(self, params):
        def _rollback(record_vo: Record):
            _LOGGER.info(
                f'[ROLLBACK] Delete Record : {record_vo.record_id} ({record_vo.cloud_service_id})')
            record_vo.delete()

        record_vo: Record = self.record_model.create(params)
        self.transaction.add_rollback(_rollback, record_vo)

        return record_vo

    def delete_record(self, record_id, domain_id):
        record_vo = self.get_record(record_id, domain_id)
        record_vo.delete()

    def get_record(self, record_id, domain_id, only=None):
        return self.record_model.get(record_id=record_id, domain_id=domain_id, only=only)

    def filter_records(self, **conditions):
        return self.record_model.filter(**conditions)

    def list_records(self, query={}):
        return self.record_model.query(**query)

    def stat_records(self, query):
        return self.record_model.stat(**query)
