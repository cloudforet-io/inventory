import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.region_model import Region, RegionMemberMap

_LOGGER = logging.getLogger(__name__)


class RegionManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region_model: Region = self.locator.get_model('Region')

    def create_region(self, params):
        def _rollback(region_vo):
            _LOGGER.info(f'[ROLLBACK] Delete region : {region_vo.name} ({region_vo.region_id})')
            region_vo.delete()

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

    def add_member(self, region_vo, user_id, labels):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["region_id"]})')
            region_vo.update(old_data)

        self.transaction.add_rollback(_rollback, region_vo.to_dict())

        return region_vo.append('members', {'user_id': user_id, 'labels': labels})

    def modify_member(self, region_vo, user_info, labels=None):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["region_id"]})')
            region_vo.update(old_data)

        self.transaction.add_rollback(_rollback, region_vo.to_dict())
        region_member_model: RegionMemberMap = self.locator.get_model('RegionMemberMap')
        user_id = user_info['user_id']

        region_map_vos, map_total_count = region_member_model.query({
            'filter': [{
                'k': 'region',
                'v': region_vo,
                'o': 'eq'
            }, {
                'k': 'user_id',
                'v': user_id,
                'o': 'eq'
            }]
        })

        region_map_vo = region_map_vos[0]

        update_dic = {'user_id': user_id}

        if labels is not None:
            update_dic['labels'] = labels

        return region_map_vo.update(update_dic)

    def remove_member(self, region_vo, user_id):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["region_id"]})')
            region_vo.update(old_data)

        self.transaction.add_rollback(_rollback, region_vo.to_dict())
        region_vo.remove('members', user_id)

    def get_region(self, region_id, domain_id, only=None):
        return self.region_model.get(region_id=region_id, domain_id=domain_id, only=only)

    def list_regions(self, query):
        return self.region_model.query(**query)

    def stat_regions(self, query):
        return self.region_model.stat(**query)

    def list_region_maps(self, query):
        region_member_model: RegionMemberMap = self.locator.get_model('RegionMemberMap')
        return region_member_model.query(**query)

    @staticmethod
    def delete_region_by_vo(region_vo):
        region_vo.delete()
