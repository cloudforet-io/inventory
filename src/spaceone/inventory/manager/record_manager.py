import logging
from typing import Tuple

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.record_model import Record

_LOGGER = logging.getLogger(__name__)


class RecordManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.record_model: Record = self.locator.get_model("Record")

    def create_record(self, params: dict) -> Record:
        def _rollback(vo: Record):
            _LOGGER.info(
                f"[ROLLBACK] Delete Record : {vo.record_id} ({vo.cloud_service_id})"
            )
            vo.delete()

        record_vo: Record = self.record_model.create(params)
        self.transaction.add_rollback(_rollback, record_vo)

        return record_vo

    def get_record(self, record_id: str, domain_id: str) -> Record:
        return self.record_model.get(record_id=record_id, domain_id=domain_id)

    def filter_records(self, **conditions) -> QuerySet:
        return self.record_model.filter(**conditions)

    def list_records(self, query: dict) -> Tuple[QuerySet, int]:
        return self.record_model.query(**query)

    def stat_records(self, query: dict) -> dict:
        return self.record_model.stat(**query)
