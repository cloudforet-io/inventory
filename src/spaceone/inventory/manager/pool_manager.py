import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.pool_model import Pool, PoolMemberMap

_LOGGER = logging.getLogger(__name__)


class PoolManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pool_model: Pool = self.locator.get_model('Pool')

    def create_pool(self, params):
        def _rollback(pool_vo):
            _LOGGER.info(f'[ROLLBACK] Delete pool : {pool_vo.name} ({pool_vo.pool_id})')
            pool_vo.delete()

        pool_vo: Pool = self.pool_model.create(params)
        self.transaction.add_rollback(_rollback, pool_vo)

        return pool_vo

    def update_pool(self, params):
        return self.update_pool_by_vo(params,
                                      self.get_pool(params.get('pool_id'),
                                                    params.get('domain_id')))

    def update_pool_by_vo(self, params, pool_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("name")} ({old_data.get("pool_id")})')
            pool_vo.update(old_data)

        self.transaction.add_rollback(_rollback, pool_vo.to_dict())

        return pool_vo.update(params)

    def delete_pool(self, pool_id, domain_id):
        self.delete_pool_by_vo(self.get_pool(pool_id, domain_id))

    def add_member(self, pool_vo, user_id, labels):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("name")} ({old_data.get("pool_id")})')
            pool_vo.update(old_data)

        self.transaction.add_rollback(_rollback, pool_vo.to_dict())

        return pool_vo.append('members', {'user_id': user_id, 'labels': labels})

    def modify_member(self, pool_vo, user_info, labels=None):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("name")} ({old_data.get("pool_id")})')
            pool_vo.update(old_data)

        self.transaction.add_rollback(_rollback, pool_vo.to_dict())
        pool_member_map_model: PoolMemberMap = self.locator.get_model('PoolMemberMap')
        user_id = user_info['user_id']

        pool_map_vos, map_total_count = pool_member_map_model.query({
            'filter': [{
                'k': 'pool',
                'v': pool_vo,
                'o': 'eq'
            }, {
                'k': 'user_id',
                'v': user_id,
                'o': 'eq'
            }]
        })

        pool_map_vo = pool_map_vos[0]

        update_dic = {'user_id': user_id}

        if labels is not None:
            update_dic['labels'] = labels

        return pool_map_vo.update(update_dic)

    def remove_member(self, pool_vo, user_id):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("name")} ({old_data.get("pool_id")})')
            pool_vo.update(old_data)

        self.transaction.add_rollback(_rollback, pool_vo.to_dict())
        pool_vo.remove('members', user_id)

    def get_pool(self, pool_id, domain_id, only=None):
        return self.pool_model.get(pool_id=pool_id, domain_id=domain_id, only=only)

    def list_pools(self, query):
        return self.pool_model.query(**query)

    def stat_pools(self, query):
        return self.pool_model.stat(**query)

    def list_pool_maps(self, query):
        pool_member_model: PoolMemberMap = self.locator.get_model('PoolMemberMap')
        return pool_member_model.query(**query)

    @staticmethod
    def delete_pool_by_vo(pool_vo):
        pool_vo.delete()

