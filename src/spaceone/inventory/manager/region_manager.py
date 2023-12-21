import logging
from typing import Tuple

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.inventory.lib.resource_manager import ResourceManager
from spaceone.inventory.model.region_model import Region

_LOGGER = logging.getLogger(__name__)


class RegionManager(BaseManager, ResourceManager):
    resource_keys = ["region_id"]
    query_method = "list_regions"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region_model: Region = self.locator.get_model("Region")

    def create_region(self, params: dict) -> Region:
        def _rollback(vo: Region):
            _LOGGER.info(f"[ROLLBACK] Delete region : {vo.name} ({vo.region_id})")
            vo.delete()

        region_vo: Region = self.region_model.create(params)
        self.transaction.add_rollback(_rollback, region_vo)

        return region_vo

    def update_region_by_vo(self, params: dict, region_vo: Region) -> Region:
        def _rollback(old_data):
            _LOGGER.info(
                f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["region_id"]})'
            )
            region_vo.update(old_data)

        self.transaction.add_rollback(_rollback, region_vo.to_dict())
        return region_vo.update(params)

    @staticmethod
    def delete_region_by_vo(region_vo: Region) -> None:
        region_vo.delete()

    def get_region(
        self, region_id: str, domain_id: str, workspace_id: str = None
    ) -> Region:
        conditions = {"region_id": region_id, "domain_id": domain_id}

        if workspace_id:
            conditions.update({"workspace_id": workspace_id})

        return self.region_model.get(**conditions)

    def filter_regions(self, **conditions) -> QuerySet:
        return self.region_model.filter(**conditions)

    def list_regions(self, query: dict) -> Tuple[QuerySet, int]:
        return self.region_model.query(**query)

    def stat_regions(self, query: dict) -> dict:
        return self.region_model.stat(**query)
