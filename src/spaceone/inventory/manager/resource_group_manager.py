import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.lib.resource_manager import ResourceManager
from spaceone.inventory.model.resource_group_model import ResourceGroup

_LOGGER = logging.getLogger(__name__)


class ResourceGroupManager(BaseManager, ResourceManager):

    resource_keys = ['resource_group_id']
    query_method = 'list_resource_groups'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_group_model: ResourceGroup = self.locator.get_model('ResourceGroup')

    def create_resource_group(self, params):
        def _rollback(rg_vo):
            _LOGGER.info(f'[ROLLBACK] Delete resource group : {rg_vo.name} ({rg_vo.resource_group_id})')
            rg_vo.delete()

        rg_vo: ResourceGroup = self.resource_group_model.create(params)
        self.transaction.add_rollback(_rollback, rg_vo)

        return rg_vo

    def update_resource_group(self, params):
        return self.update_resource_group_by_vo(params, self.get_resource_group(params['resource_group_id'],
                                                                                params['domain_id']))

    def update_resource_group_by_vo(self, params, rg_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data["name"]} ({old_data["resource_group_id"]})')
            rg_vo.update(old_data)

        self.transaction.add_rollback(_rollback, rg_vo.to_dict())
        return rg_vo.update(params)

    def delete_resource_group(self, resource_group_id, domain_id):
        self.delete_resource_group_by_vo(self.get_resource_group(resource_group_id, domain_id))

    def get_resource_group(self, resource_group_id, domain_id, only=None):
        return self.resource_group_model.get(resource_group_id=resource_group_id, domain_id=domain_id, only=only)

    def list_resource_groups(self, query):
        return self.resource_group_model.query(**query)

    def stat_resource_groups(self, query):
        return self.resource_group_model.stat(**query)

    @staticmethod
    def delete_resource_group_by_vo(resource_group_vo):
        resource_group_vo.delete()

    def get_resource_group_filter(self, resource_group_id, resource_type, domain_id, keyword_filter):
        filters = []
        resource_group_vo = self.get_resource_group(resource_group_id, domain_id)
        for resource in resource_group_vo.resources:
            if resource.resource_type.startswith(resource_type):
                _filter = self._get_filter_from_resource_type(resource.resource_type)
                _filter += resource.filter

                _filter.append({
                    'k': 'domain_id',
                    'v': domain_id,
                    'o': 'eq'
                })

                if resource_group_vo.project_id:
                    _filter.append({
                        'k': 'project_id',
                        'v': resource_group_vo.project_id,
                        'o': 'eq'
                    })

                query = {
                    'filter': _filter
                }

                if resource.keyword and resource.keyword.strip() != '':
                    query['filter_or'] = self._get_resource_group_keyword_filter(resource.keyword, keyword_filter)

                filters.append(query)

        return filters

    @staticmethod
    def _get_resource_group_keyword_filter(keyword, keyword_filter):
        filter_or = []
        for key in keyword_filter:
            filter_or.append({
                'k': key,
                'v': list(filter(None, keyword.split(' '))),
                'o': 'contain_in'
            })

        return filter_or

    def _get_filter_from_resource_type(self, resource_type):
        resource_type_split = resource_type.split('?', 1)
        if len(resource_type_split) == 2:
            return self._get_filter_from_query_string(resource_type_split[1])
        else:
            return []

    @staticmethod
    def _get_filter_from_query_string(query_string):
        _filter = []
        for kv in query_string.split('&'):
            kv_split = kv.split('=', 1)
            if len(kv_split) == 2:
                if kv_split[0].strip() != '' and kv_split[1].strip() != '':
                    _filter.append({
                        'k': kv_split[0],
                        'v': kv_split[1],
                        'o': 'eq'
                    })

        return _filter
