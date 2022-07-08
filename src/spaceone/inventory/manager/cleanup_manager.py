import logging
import re

from datetime import datetime, timedelta

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.inventory.manager.collection_state_manager import CollectionStateManager

_LOGGER = logging.getLogger(__name__)


RESOURCE_MAP = {
    'inventory.CloudService': 'CloudServiceManager',
    'inventory.CloudServiceType': 'CloudServiceTypeManager',
    'inventory.Region': 'RegionManager',
}

OP_MAP = {
    '=': 'eq',
    '>=': 'gte',
    '<=': 'lte',
    '>': 'gt',
    '<': 'lt',
    '!=': 'not'
}


class CleanupManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _increment_disconnected_count_by_collector(self, state_mgr, collector_id, secret_id, job_task_id, domain_id):
        updated_at = datetime.utcnow() - timedelta(hours=1)

        query = {
            'filter': [
                {'k': 'collector_id', 'v': collector_id, 'o': 'eq'},
                {'k': 'secret_id', 'v': secret_id, 'o': 'eq'},
                {'k': 'job_task_id', 'v': job_task_id, 'o': 'not'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'updated_at', 'v': updated_at, 'o': 'lt'},
            ]
        }

        _LOGGER.debug(f'[_increment_disconnected_count_by_collector] query: {query}')
        state_vos, total_count = state_mgr.list_collection_states(query)
        _LOGGER.debug(f'[_increment_disconnected_count_by_collector] total_count: {total_count}')
        state_vos.increment('disconnected_count')

        return total_count

    def _delete_resources_by_collector(self, state_mgr, collector_id, domain_id):
        disconnected_count = config.get_global('DEFAULT_DISCONNECTED_STATE_DELETE_POLICY', 3)

        query = {
            'filter': [
                {'k': 'collector_id', 'v': collector_id, 'o': 'eq'},
                {'k': 'disconnected_count', 'v': disconnected_count, 'o': 'gte'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
            ]
        }

        state_vos, total_count = state_mgr.list_collection_states(query)
        cloud_service_ids = []
        for state_vo in state_vos:
            if state_vo.resource_type == 'inventory.CloudService':
                cloud_service_ids.append(state_vo.resource_id)

        total_deleted_count = 0

        if len(cloud_service_ids) > 0:
            cloud_svc_mgr = self.locator.get_manager('CloudServiceManager')

            query = {
                'filter': [
                    {'k': 'cloud_service_id', 'v': cloud_service_ids, 'o': 'in'},
                    {'k': 'domain_id', 'v': domain_id, 'o': 'eq'}
                ]
            }

            try:
                deleted_count = cloud_svc_mgr.delete_resources(query)
                _LOGGER.debug(f'[_delete_resources_by_collector] delete cloud service {deleted_count} in {domain_id}')
                total_deleted_count += deleted_count
            except Exception as e:
                _LOGGER.error(f'[_delete_resources_by_collector] delete cloud service error: {e}', exc_info=True)

        return total_deleted_count

    def update_collection_state(self, collector_id, secret_id, job_task_id, domain_id):
        """ Delete resource which does not have same job_task_id
        """
        state_mgr: CollectionStateManager = self.locator.get_manager('CollectionStateManager')
        disconnected_count = self._increment_disconnected_count_by_collector(state_mgr, collector_id, secret_id,
                                                                             job_task_id, domain_id)
        deleted_count = self._delete_resources_by_collector(state_mgr, collector_id, domain_id)

        return (disconnected_count - deleted_count), deleted_count

    def delete_resources_by_policy(self, resource_type, hour, domain_id):
        """ List resources
            updated_at < now - time delta(hour)

        Returns: deleted_count
        """

        updated_at = datetime.utcnow() - timedelta(hours=hour)
        resource_type_name, _filter = self._parse_resource_type(resource_type)

        _filter.extend(
            [
                {'k': 'updated_at', 'v': updated_at, 'o': 'lt'},
                {'k': 'domain_id',  'v': domain_id, 'o': 'eq'}
            ]
        )

        if resource_type_name in ['inventory.CloudServiceType', 'inventory.Region']:
            _filter.append({
                'k': 'updated_by',
                'v': 'manual',
                'o': 'not'
            })

        query = {'filter': _filter}

        _LOGGER.debug(f'[delete_resources_by_policy] query: {query}')
        if resource_type_name not in RESOURCE_MAP:
            _LOGGER.error(f'[delete_resources_by_policy] not found {resource_type_name}')
            return 0

        mgr_name = RESOURCE_MAP[resource_type_name]
        mgr = self.locator.get_manager(mgr_name)
        try:
            deleted_count = mgr.delete_resources(query)
            _LOGGER.debug(f'[delete_resources_by_policy] {deleted_count} in {domain_id}')
            return deleted_count
        except Exception as e:
            _LOGGER.error(f'[delete_resources] {e}', exc_info=True)
            return 0

    @staticmethod
    def _parse_resource_type(resource_type):
        """
        Example of resource type
         - inventory.CloudService
         - inventory.CloudService?provider=aws&cloud_service_group=DynamoDB
        """
        items = resource_type.split('?')
        resource_type_name = items[0]
        _filter = []
        if len(items) == 2:
            kv = items[1]
            kv_items = kv.split('&')
            for kv_item in kv_items:
                item = re.compile(r'(?P<k>[\w.]+)(?P<o>=|!=|<=|<|>=|>)(?P<v>[\w.]*)')
                m = item.match(kv_item)
                kvo = m.groupdict()
                if kvo['v'] == '':
                    value = None
                else:
                    value = kvo['v']
                _filter.append({'k': kvo['k'], 'o': OP_MAP[kvo["o"]], 'v': value})
                # item = kv_item.split('=')
                # if len(item) == 2:
                #    filter_list.append((item[0], item[1]))
        elif len(items) > 2:
            # Raise wrong format
            _LOGGER.error(f'[_parse_resource_type] wrong format: {resource_type}')

        _LOGGER.debug(f'[_parse_resource_type] {resource_type} -> {resource_type_name}, {_filter}')
        return resource_type_name, _filter
