import logging
from spaceone.core.service import *
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.zone_manager import ZoneManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class RegionService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.region_mgr: RegionManager = self.locator.get_manager('RegionManager')

    @transaction
    @check_required(['name', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            region_vo (object)

        """

        region_mgr: RegionManager = self.locator.get_manager('RegionManager')
        return region_mgr.create_region(params)

    @transaction
    @check_required(['region_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'name': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            region_vo (object)

        """

        return self.region_mgr.update_region_by_vo(params, region_vo=self.region_mgr.get_region(params['region_id'],
                                                                                                params['domain_id']))

    @transaction
    @check_required(['region_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        region_vo = self.region_mgr.get_region(params['region_id'], params['domain_id'])
        self._check_exist_resource(region_vo)
        self.region_mgr.delete_region_by_vo(region_vo)

    @transaction
    @check_required(['region_id', 'user_id', 'domain_id'])
    def add_member(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'user_id': 'str',
                    'labels': 'list',
                    'domain_id': 'str'
                }

        Returns:
            region_member_vo (object)

        """
        region_id = params['region_id']
        user_id = params['user_id']
        domain_id = params['domain_id']

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        region_vo = self.region_mgr.get_region(region_id, domain_id)
        user_info = identity_mgr.get_user(user_id, domain_id)

        labels = list(set(params.get('labels', [])))

        self._check_not_exist_user_in_region(region_id, user_id)
        region_vo = self.region_mgr.add_member(region_vo, user_id, labels)

        return {
            'region': region_vo,
            'user': user_info,
            'labels': labels
        }

    @transaction
    @check_required(['region_id', 'user_id', 'domain_id'])
    def modify_member(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'user_id': 'str',
                    'labels': 'list',
                    'domain_id': 'str'
                }

        Returns:
            region_member_vo (object)

        """
        region_id = params['region_id']
        user_id = params['user_id']
        domain_id = params['domain_id']

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        region_vo = self.region_mgr.get_region(region_id, domain_id)
        user_info = identity_mgr.get_user(user_id, domain_id)

        self._check_exist_user_in_region(region_id, user_id)

        if 'labels' in params:
            labels = list(set(params['labels']))
        else:
            labels = None

        region_map_vo = self.region_mgr.modify_member(region_vo, user_info, labels)

        return {
            'region': region_vo,
            'user': user_info,
            'labels': region_map_vo.labels
        }

    @transaction
    @check_required(['region_id', 'user_id', 'domain_id'])
    def remove_member(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'user_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """
        region_id = params['region_id']
        user_id = params['user_id']
        domain_id = params['domain_id']

        region_vo = self.region_mgr.get_region(region_id, domain_id)

        self._check_exist_user_in_region(region_id, user_id)
        self.region_mgr.remove_member(region_vo, user_id)

    @transaction
    @check_required(['region_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            region_vo (object)

        """

        return self.region_mgr.get_region(params['region_id'], params['domain_id'], params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['region_id', 'name', 'domain_id'])
    @append_keyword_filter(['region_id', 'name'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'region_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.region_mgr.list_regions(params.get('query', {}))

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
        return self.region_mgr.stat_regions(query)

    @transaction
    @check_required(['region_id', 'domain_id'])
    @append_query_filter(['user_id', 'domain_id'])
    def list_members(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
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

        region_vo = self.region_mgr.get_region(params['region_id'], params['domain_id'])

        _query = {
            'filter': [{
                'k': 'region',
                'v': region_vo,
                'o': 'eq'
            },{
                'k': 'user_id',
                'v': list(map(lambda user: user['user_id'], users)),
                'o': 'in'
            }]
        }

        region_map_vos, total_count = self.region_mgr.list_region_maps(_query)

        region_members = []
        for user in users:
            for region_map_vo in region_map_vos:
                if region_map_vo.user_id == user['user_id']:
                    _dic = {'region': region_map_vo.region, 'labels': region_map_vo.labels, 'user': user}
                    region_members.append(_dic)
                    break

        return region_members, total_count

    def _check_exist_resource(self, region_vo):
        zone_mgr: ZoneManager = self.locator.get_manager('ZoneManager')
        zone_vos, total_count = zone_mgr.list_zones({'filter': [{'k': 'region_id',
                                                                 'v': region_vo.region_id,
                                                                 'o': 'eq'}]})

        if total_count > 0:
            raise ERROR_EXIST_RESOURCE(key='zone_id', value=zone_vos[0].zone_id)

    def _check_exist_user_in_region(self, region_id, user_id):
        query = {
            'filter': [
                {'k': 'region_id', 'v': region_id, 'o': 'eq'},
                {'k': 'user_id', 'v': user_id, 'o': 'eq'}
            ]
        }

        region_map_vos, total_count = self.region_mgr.list_region_maps(query)

        if total_count == 0:
            raise ERROR_NOT_FOUND_USER_IN_REGION(user_id=user_id, region_id=region_id)

    def _check_not_exist_user_in_region(self, region_id, user_id):
        query = {
            'filter': [
                {'k': 'region_id', 'v': region_id, 'o': 'eq'},
                {'k': 'user_id', 'v': user_id, 'o': 'eq'}
            ]
        }

        region_map_vos, total_count = self.region_mgr.list_region_maps(query)

        if total_count > 0:
            raise ERROR_ALREADY_EXIST_USER_IN_REGION(user_id=user_id, region_id=region_id)
