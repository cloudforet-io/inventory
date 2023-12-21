import logging
from typing import Tuple
from datetime import datetime
from spaceone.core.manager import BaseManager
from spaceone.core.model.mongo_model import QuerySet
from spaceone.inventory.model.collector_model import Collector


__ALL__ = ["CollectorManager"]

_LOGGER = logging.getLogger(__name__)


class CollectorManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collector_model: Collector = self.locator.get_model("Collector")

    def create_collector(self, params: dict) -> Collector:
        def _rollback(vo: Collector):
            _LOGGER.info(f"[ROLLBACK] Delete collector : {vo.name} ({vo.collector_id})")
            vo.delete()

        collector_vo: Collector = self.collector_model.create(params)
        self.transaction.add_rollback(_rollback, collector_vo)
        return collector_vo

    def update_collector_by_vo(
        self, collector_vo: Collector, params: dict
    ) -> Collector:
        def _rollback(old_data):
            _LOGGER.info(f"[ROLLBACK] Revert Data : {old_data.get('collector_id')}")
            collector_vo.update(old_data)

        self.transaction.add_rollback(_rollback, collector_vo.to_dict())
        return collector_vo.update(params)

    def enable_collector(
        self, collector_id: str, domain_id: str, workspace_id: str = None
    ):
        collector_vo: Collector = self.collector_model.get(
            collector_id=collector_id, domain_id=domain_id, workspace_id=workspace_id
        )

        return self.update_collector_by_vo(collector_vo, {"state": "ENABLED"})

    def disable_collector(
        self, collector_id: str, domain_id: str, workspace_id: str = None
    ):
        collector_vo: Collector = self.collector_model.get(
            collector_id=collector_id, domain_id=domain_id, workspace_id=workspace_id
        )

        return self.update_collector_by_vo(collector_vo, {"state": "DISABLED"})

    @staticmethod
    def delete_collector_by_vo(collector_vo: Collector) -> None:
        collector_vo.delete()

    def get_collector(
        self,
        collector_id: str,
        domain_id: str,
        workspace_id: str = None,
    ) -> Collector:
        conditions = {
            "collector_id": collector_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions.update({"workspace_id": workspace_id})

        return self.collector_model.get(**conditions)

    def filter_collector(self, **conditions) -> QuerySet:
        return self.collector_model.filter(**conditions)

    def list_collectors(self, query: dict) -> Tuple[QuerySet, int]:
        return self.collector_model.query(**query)

    def stat_collectors(self, query: dict) -> dict:
        return self.collector_model.stat(**query)

    def update_last_collected_time(self, collector_vo: Collector):
        self.update_collector_by_vo(
            collector_vo, {"last_collected_at": datetime.utcnow()}
        )
