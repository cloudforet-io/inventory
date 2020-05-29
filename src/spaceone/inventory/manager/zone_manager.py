import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.zone_model import Zone, ZoneMemberMap

_LOGGER = logging.getLogger(__name__)


class ZoneManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.zone_model: Zone = self.locator.get_model('Zone')

    def create_zone(self, params):
        def _rollback(zone_vo):
            _LOGGER.info(f'[ROLLBACK] Delete zone : {zone_vo.name} ({zone_vo.zone_id})')
            zone_vo.delete()

        zone_vo: Zone = self.zone_model.create(params)
        self.transaction.add_rollback(_rollback, zone_vo)

        return zone_vo

    def update_zone(self, params):
        return self.update_zone_by_vo(params, self.get_zone(params['zone_id'], params['domain_id']))

    def update_zone_by_vo(self, params, zone_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["zone_id"]})')
            zone_vo.update(old_data)

        self.transaction.add_rollback(_rollback, zone_vo.to_dict())

        return zone_vo.update(params)

    def delete_zone(self, zone_id, domain_id):
        self.delete_zone_by_vo(self.get_zone(zone_id, domain_id))

    def add_member(self, zone_vo, user_id, labels):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["zone_id"]})')
            zone_vo.update(old_data)

        self.transaction.add_rollback(_rollback, zone_vo.to_dict())

        return zone_vo.append('members', {'user_id': user_id, 'labels': labels})

    def modify_member(self, zone_vo, user_info, labels=None):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["zone_id"]})')
            zone_vo.update(old_data)

        self.transaction.add_rollback(_rollback, zone_vo.to_dict())
        zone_member_map_model: ZoneMemberMap = self.locator.get_model('ZoneMemberMap')
        user_id = user_info['user_id']

        zone_map_vos, map_total_count = zone_member_map_model.query({
            'filter': [{
                'k': 'zone',
                'v': zone_vo,
                'o': 'eq'
            }, {
                'k': 'user_id',
                'v': user_id,
                'o': 'eq'
            }]
        })

        zone_map_vo = zone_map_vos[0]

        update_dic = {'user_id': user_id}

        if labels is not None:
            update_dic['labels'] = labels

        return zone_map_vo.update(update_dic)

    def remove_member(self, zone_vo, user_id):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["zone_id"]})')
            zone_vo.update(old_data)

        self.transaction.add_rollback(_rollback, zone_vo.to_dict())
        zone_vo.remove('members', user_id)

    def get_zone(self, zone_id, domain_id, only=None):
        return self.zone_model.get(zone_id=zone_id, domain_id=domain_id, only=only)

    def list_zones(self, query):
        return self.zone_model.query(**query)

    def stat_zones(self, query):
        return self.zone_model.stat(**query)

    def list_zone_maps(self, query):
        zone_member_model: ZoneMemberMap = self.locator.get_model('ZoneMemberMap')
        return zone_member_model.query(**query)

    @staticmethod
    def delete_zone_by_vo(zone_vo):
        zone_vo.delete()