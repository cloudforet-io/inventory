import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.lib.resource_manager import ResourceManager
from spaceone.inventory.model.region_model import Region

_LOGGER = logging.getLogger(__name__)


class RegionManager(BaseManager, ResourceManager):

    resource_keys = ['region_id']
    query_method = 'list_regions'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region_model: Region = self.locator.get_model('Region')

    def create_region(self, params):
        def _rollback(region_vo):
            _LOGGER.info(f'[ROLLBACK] Delete region : {region_vo.name} ({region_vo.region_id})')
            region_vo.delete()

        params['region_ref'] = f'{params["region_type"]}.{params["region_code"]}'

        region_vo: Region = self.region_model.create(params)
        self.transaction.add_rollback(_rollback, region_vo)

        return region_vo

    def update_region(self, params):
        return self.update_region_by_vo(params, self.get_region(params['region_id'], params['domain_id']))

    def update_region_by_vo(self, params, region_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["region_id"]})')
            region_vo.update(old_data)

        self.transaction.add_rollback(_rollback, region_vo.to_dict())
        return region_vo.update(params)

    def delete_region(self, region_id, domain_id):
        self.delete_region_by_vo(self.get_region(region_id, domain_id))

    def get_region(self, region_id, domain_id, only=None):
        return self.region_model.get(region_id=region_id, domain_id=domain_id, only=only)

    def get_region_from_code(self, region_code, region_type, domain_id, only=None):
        return self.region_model.get(region_code=region_code, region_type=region_type, domain_id=domain_id, only=only)

    def list_regions(self, query):
        return self.region_model.query(**query)

    def stat_regions(self, query):
        return self.region_model.stat(**query)

    @staticmethod
    def delete_region_by_vo(region_vo):
        region_vo.delete()
