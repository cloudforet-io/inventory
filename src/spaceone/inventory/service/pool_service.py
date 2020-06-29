from spaceone.core.service import *
from spaceone.inventory.manager.zone_manager import ZoneManager
from spaceone.inventory.manager.pool_manager import PoolManager
from spaceone.inventory.manager.server_manager import ServerManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.error import *


@authentication_handler
@authorization_handler
@event_handler
class PoolService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.pool_mgr: PoolManager = self.locator.get_manager('PoolManager')

    @transaction
    @check_required(['name', 'zone_id', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'tags': 'dict',
                    'zone_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            pool_vo (object)

        """

        zone_mgr: ZoneManager = self.locator.get_manager('ZoneManager')
        zone_vo = zone_mgr.get_zone(params.get('zone_id'), params.get('domain_id'))
        params['zone'] = zone_vo
        params['region'] = zone_vo.region
        return self.pool_mgr.create_pool(params)

    @transaction
    @check_required(['pool_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'pool_id': 'str',
                    'name': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            pool_vo (object)

        """

        return self.pool_mgr.update_pool_by_vo(params,
                                               pool_vo=self.pool_mgr.get_pool(params.get('pool_id'),
                                                                              params.get('domain_id')))

    @transaction
    @check_required(['pool_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'pool_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        pool_vo = self.pool_mgr.get_pool(params.get('pool_id'), params.get('domain_id'))
        self._check_exist_resource(pool_vo)
        self.pool_mgr.delete_pool_by_vo(pool_vo)

    @transaction
    @check_required(['pool_id', 'user_id', 'domain_id'])
    def add_member(self, params):
        """
        Args:
            params (dict): {
                    'pool_id': 'str',
                    'user_id': 'str',
                    'labels': 'list',
                    'domain_id': 'str'
                }

        Returns:
            pool_member_vo (object)

        """
        pool_id = params['pool_id']
        user_id = params['user_id']
        domain_id = params['domain_id']

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        pool_vo = self.pool_mgr.get_pool(pool_id, domain_id)
        user_info = identity_mgr.get_user(user_id, domain_id)

        labels = list(set(params.get('labels', [])))

        self._check_not_exist_user_in_pool(pool_id, user_id)
        pool_vo = self.pool_mgr.add_member(pool_vo, user_id, labels)

        return {
            'pool': pool_vo,
            'user': user_info,
            'labels': labels
        }

    @transaction
    @check_required(['pool_id', 'user_id', 'domain_id'])
    def modify_member(self, params):
        """
        Args:
            params (dict): {
                    'pool_id': 'str',
                    'user_id': 'str',
                    'labels': 'list',
                    'domain_id': 'str'
                }

        Returns:
            pool_member_vo (object)

        """
        pool_id = params['pool_id']
        user_id = params['user_id']
        domain_id = params['domain_id']

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        pool_vo = self.pool_mgr.get_pool(pool_id, domain_id)
        user_info = identity_mgr.get_user(user_id, domain_id)

        self._check_exist_user_in_pool(pool_id, user_id)

        if 'labels' in params:
            labels = list(set(params['labels']))
        else:
            labels = None

        pool_map_vo = self.pool_mgr.modify_member(pool_vo, user_info, labels)

        return {
            'pool': pool_vo,
            'user': user_info,
            'labels': pool_map_vo.labels
        }

    @transaction
    @check_required(['pool_id', 'user_id', 'domain_id'])
    def remove_member(self, params):
        """
        Args:
            params (dict): {
                    'pool_id': 'str',
                    'user_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """
        pool_id = params['pool_id']
        user_id = params['user_id']
        domain_id = params['domain_id']

        pool_vo = self.pool_mgr.get_pool(pool_id, domain_id)

        self._check_exist_user_in_pool(pool_id, user_id)
        self.pool_mgr.remove_member(pool_vo, user_id)

    @transaction
    @check_required(['pool_id', 'domain_id'])
    @change_only_key({'region_info': 'region', 'zone_info': 'zone'})
    def get(self, params):
        """
        Args:
            params (dict): {
                    'pool_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            pool_vo (object)

        """

        return self.pool_mgr.get_pool(params['pool_id'], params['domain_id'], params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @change_only_key({'region_info': 'region', 'zone_info': 'zone'}, key_path='query.only')
    @append_query_filter(['region_id', 'zone_id', 'pool_id', 'name', 'domain_id'])
    @append_keyword_filter(['pool_id', 'name'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'pool_id': 'str',
                    'zone_id': 'str',
                    'region_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.pool_mgr.list_pools(params.get('query', {}))

    @transaction
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.pool_mgr.stat_pools(query)

    @transaction
    @check_required(['pool_id', 'domain_id'])
    @append_query_filter(['user_id', 'domain_id'])
    def list_members(self, params):
        """
        Args:
            params (dict): {
                    'pool_id': 'str',
                    'user_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict'
                }

        Returns:
            results (list)
            total_count (int)

        """

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')

        query = params.get('query', {})
        if 'page' in query:
            if 'start' in query['page']:
                query['page']['start'] = int(query['page']['start'])

            if 'limit' in query['page']:
                query['page']['limit'] = int(query['page']['limit'])

        response = identity_mgr.list_users(query, params['domain_id'])
        users = response.get('results', [])

        pool_vo = self.pool_mgr.get_pool(params['pool_id'], params['domain_id'])

        _query = {
            'filter': [{
                'k': 'pool',
                'v': pool_vo,
                'o': 'eq'
            }, {
                'k': 'user_id',
                'v': list(map(lambda user: user['user_id'], users)),
                'o': 'in'
            }]
        }

        pool_map_vos, total_count = self.pool_mgr.list_pool_maps(_query)

        pool_members = []
        for user in users:
            for pool_map_vo in pool_map_vos:
                if pool_map_vo.user_id == user['user_id']:
                    _dic = {'pool': pool_map_vo.pool, 'labels': pool_map_vo.labels, 'user': user}
                    pool_members.append(_dic)
                    break

        return pool_members, total_count

    def _check_servers_in_pool(self, pool_vo):
        svr_mgr: ServerManager = self.locator.get_manager('ServerManager')
        svr_vos, total_count = svr_mgr.list_servers({'filter': [{'k': 'pool', 'v': pool_vo.id, 'o': 'eq'}]})

        if total_count > 0:
            raise ERROR_EXIST_RESOURCE(key='server_id', value=svr_vos[0].server_id)

    def _check_exist_resource(self, pool_vo):
        self._check_servers_in_pool(pool_vo)

    def _check_exist_user_in_pool(self, pool_id, user_id):
        query = {
            'filter': [
                {'k': 'pool_id', 'v': pool_id, 'o': 'eq'},
                {'k': 'user_id', 'v': user_id, 'o': 'eq'}
            ]
        }

        pool_map_vos, total_count = self.pool_mgr.list_pool_maps(query)

        if total_count == 0:
            raise ERROR_NOT_FOUND_USER_IN_POOL(user_id=user_id, pool_id=pool_id)

    def _check_not_exist_user_in_pool(self, pool_id, user_id):
        query = {
            'filter': [
                {'k': 'pool_id', 'v': pool_id, 'o': 'eq'},
                {'k': 'user_id', 'v': user_id, 'o': 'eq'}
            ]
        }

        pool_map_vos, total_count = self.pool_mgr.list_pool_maps(query)

        if total_count > 0:
            raise ERROR_ALREADY_EXIST_USER_IN_POOL(user_id=user_id, pool_id=pool_id)
