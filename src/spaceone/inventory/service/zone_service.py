from spaceone.core.service import *
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.zone_manager import ZoneManager
from spaceone.inventory.manager.pool_manager import PoolManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.error import *


@authentication_handler
@authorization_handler
@event_handler
class ZoneService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.zone_mgr: ZoneManager = self.locator.get_manager('ZoneManager')

    @transaction
    @check_required(['name', 'region_id', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'tags': 'dict',
                    'region_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            zone_vo (object)

        """

        region_mgr: RegionManager = self.locator.get_manager('RegionManager')
        params['region'] = region_mgr.get_region(params['region_id'], params['domain_id'])
        return self.zone_mgr.create_zone(params)

    @transaction
    @check_required(['zone_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'zone_id': 'str',
                    'name': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            zone_vo (object)

        """

        return self.zone_mgr.update_zone_by_vo(params,
                                               zone_vo=self.zone_mgr.get_zone(params['zone_id'], params['domain_id']))

    @transaction
    @check_required(['zone_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'zone_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        zone_vo = self.zone_mgr.get_zone(params['zone_id'], params['domain_id'])
        self._check_exist_resource(zone_vo)
        self.zone_mgr.delete_zone_by_vo(zone_vo)

    @transaction
    @check_required(['zone_id', 'user_id', 'domain_id'])
    def add_member(self, params):
        """
        Args:
            params (dict): {
                    'zone_id': 'str',
                    'user_id': 'str',
                    'labels': 'list',
                    'domain_id': 'str'
                }

        Returns:
            zone_member_vo (object)

        """
        zone_id = params['zone_id']
        user_id = params['user_id']
        domain_id = params['domain_id']

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        zone_vo = self.zone_mgr.get_zone(zone_id, domain_id)
        user_info = identity_mgr.get_user(user_id, domain_id)

        labels = list(set(params.get('labels', [])))

        self._check_not_exist_user_in_zone(zone_id, user_id)
        zone_vo = self.zone_mgr.add_member(zone_vo, user_id, labels)

        return {
            'zone': zone_vo,
            'user': user_info,
            'labels': labels
        }

    @transaction
    @check_required(['zone_id', 'user_id', 'domain_id'])
    def modify_member(self, params):
        """
        Args:
            params (dict): {
                    'zone_id': 'str',
                    'user_id': 'str',
                    'labels': 'list',
                    'domain_id': 'str'
                }

        Returns:
            zone_member_vo (object)

        """
        zone_id = params['zone_id']
        user_id = params['user_id']
        domain_id = params['domain_id']

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        zone_vo = self.zone_mgr.get_zone(zone_id, domain_id)
        user_info = identity_mgr.get_user(user_id, domain_id)

        self._check_exist_user_in_zone(zone_id, user_id)

        if 'labels' in params:
            labels = list(set(params['labels']))
        else:
            labels = None

        zone_map_vo = self.zone_mgr.modify_member(zone_vo, user_info, labels)

        return {
            'zone': zone_vo,
            'user': user_info,
            'labels': zone_map_vo.labels
        }

    @transaction
    @check_required(['zone_id', 'user_id', 'domain_id'])
    def remove_member(self, params):
        """
        Args:
            params (dict): {
                    'zone_id': 'str',
                    'user_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """
        zone_id = params['zone_id']
        user_id = params['user_id']
        domain_id = params['domain_id']

        zone_vo = self.zone_mgr.get_zone(zone_id, domain_id)

        self._check_exist_user_in_zone(zone_id, user_id)
        self.zone_mgr.remove_member(zone_vo, user_id)

    @transaction
    @check_required(['zone_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'zone_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            zone_vo (object)

        """

        return self.zone_mgr.get_zone(params['zone_id'], params['domain_id'], params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['region_id', 'zone_id', 'name', 'domain_id'])
    @append_keyword_filter(['zone_id', 'name'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'zone_id': 'str',
                    'region_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.zone_mgr.list_zones(params.get('query', {}))

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
        return self.zone_mgr.stat_zones(query)

    @transaction
    @check_required(['zone_id', 'domain_id'])
    @append_query_filter(['user_id', 'domain_id'])
    def list_members(self, params):
        """
        Args:
            params (dict): {
                    'zone_id': 'str',
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

        zone_vo = self.zone_mgr.get_zone(params['zone_id'], params['domain_id'])

        _query = {
            'filter': [{
                'k': 'zone',
                'v': zone_vo,
                'o': 'eq'
            },{
                'k': 'user_id',
                'v': list(map(lambda user: user['user_id'], users)),
                'o': 'in'
            }]
        }

        zone_map_vos, total_count = self.zone_mgr.list_zone_maps(_query)

        zone_members = []
        for user in users:
            for zone_map_vo in zone_map_vos:
                if zone_map_vo.user_id == user['user_id']:
                    _dic = {'zone': zone_map_vo.zone, 'labels': zone_map_vo.labels, 'user': user}
                    zone_members.append(_dic)
                    break

        return zone_members, total_count

    def _check_pools_in_zone(self, zone_vo):
        pool_mgr: PoolManager = self.locator.get_manager('PoolManager')
        pool_vos, total_count = pool_mgr.list_pools({'filter': [{'k': 'zone', 'v': zone_vo.id, 'o': 'eq'}]})

        if total_count > 0:
            raise ERROR_EXIST_RESOURCE(key='pool_id', value=pool_vos[0].pool_id)

    def _check_network_policies_in_zone(self, zone_vo):
        npolicy_mgr = self.locator.get_manager('NetworkPolicyManager')
        npolicy_vos, total_count = npolicy_mgr.list_network_policies({'filter': [{'k': 'zone',
                                                                                  'v': zone_vo.id,
                                                                                  'o': 'eq'}]})

        if total_count > 0:
            raise ERROR_EXIST_RESOURCE(key='network_policy_id', value=npolicy_vos[0].network_policy_id)

    def _check_networks_in_zone(self, zone_vo):
        net_mgr = self.locator.get_manager('NetworkManager')
        net_vos, total_count = net_mgr.list_networks({'filter': [{'k': 'zone', 'v': zone_vo.id, 'o': 'eq'}]})

        if total_count > 0:
            raise ERROR_EXIST_RESOURCE(key='network_id', value=net_vos[0].network_id)

    def _check_servers_in_zone(self, zone_vo):
        server_mgr = self.locator.get_manager('ServerManager')
        svr_vos, total_count = server_mgr.list_servers({'filter': [{'k': 'zone', 'v': zone_vo.id, 'o': 'eq'},
                                                                   {'k': 'state', 'v': 'DELETED',  'o': 'not'}]})

        if total_count > 0:
            raise ERROR_EXIST_RESOURCE(key='server_id', value=svr_vos[0].server_id)

    def _check_exist_resource(self, zone_vo):
        self._check_pools_in_zone(zone_vo)
        self._check_network_policies_in_zone(zone_vo)
        self._check_networks_in_zone(zone_vo)
        self._check_servers_in_zone(zone_vo)

    def _check_exist_user_in_zone(self, zone_id, user_id):
        query = {
            'filter': [
                {'k': 'zone_id', 'v': zone_id, 'o': 'eq'},
                {'k': 'user_id', 'v': user_id, 'o': 'eq'}
            ]
        }

        zone_map_vos, total_count = self.zone_mgr.list_zone_maps(query)

        if total_count == 0:
            raise ERROR_NOT_FOUND_USER_IN_ZONE(user_id=user_id, zone_id=zone_id)

    def _check_not_exist_user_in_zone(self, zone_id, user_id):
        query = {
            'filter': [
                {'k': 'zone_id', 'v': zone_id, 'o': 'eq'},
                {'k': 'user_id', 'v': user_id, 'o': 'eq'}
            ]
        }

        zone_map_vos, total_count = self.zone_mgr.list_zone_maps(query)

        if total_count > 0:
            raise ERROR_ALREADY_EXIST_USER_IN_ZONE(user_id=user_id, zone_id=zone_id)
