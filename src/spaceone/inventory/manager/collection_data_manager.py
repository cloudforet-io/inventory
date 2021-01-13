import logging
import copy
from datetime import datetime

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.inventory.manager.collector_manager import CollectorManager
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)
_DEFAULT_PRIORITY = 10


class CollectionDataManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.change_history = {}
        self.old_history = {}
        self.collector_priority = {}
        self.merged_data = {}
        self.is_changed = False
        self.exclude_keys = []
        self.collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        self.job_id = self.transaction.get_meta('job_id')
        self.updated_by = self.transaction.get_meta('collector_id') or 'manual'
        self.plugin_id = self.transaction.get_meta('plugin_id')
        self.update_mode = self.transaction.get_meta('update_mode') or 'REPLACE'
        self.secret_id = self.transaction.get_meta('secret.secret_id')
        self.service_account_id = self.transaction.get_meta('secret.service_account_id')
        self.updated_at = datetime.utcnow()

    def create_new_history(self, resource_data, **kwargs):
        self.exclude_keys = kwargs.get('exclude_keys', [])
        all_collectors = []
        all_service_accounts = []
        all_secrets = []

        if self.updated_by == 'manual':
            state = 'MANUAL'
        else:
            all_collectors.append(self.updated_by)
            state = 'ACTIVE'

            if self.service_account_id:
                all_service_accounts.append(self.service_account_id)

            if self.secret_id:
                all_secrets.append(self.secret_id)

        resource_data = self._change_metadata_path(resource_data)

        self._create_data_history(resource_data)

        collection_info = {
            'state': state,
            'collectors': sorted(all_collectors),
            'service_accounts': sorted(all_service_accounts),
            'secrets': sorted(all_secrets),
            'change_history': self._make_change_history(self.change_history)
        }

        resource_data['collection_info'] = collection_info
        resource_data['garbage_collection'] = {self.updated_by: self.job_id}

        return resource_data

    def _change_metadata_path(self, resource_data):
        if 'metadata' in resource_data:
            if self.updated_by == 'manual':
                resource_data['metadata'] = {
                    'manual': copy.deepcopy(resource_data['metadata'])
                }
            elif self.plugin_id:
                resource_data['metadata'] = {
                    self.plugin_id: copy.deepcopy(resource_data['metadata'])
                }
            else:
                del resource_data['metadata']

        return resource_data

    def _set_data_history(self, key, data):
        if key in self.exclude_keys:
            self._update_merge_data(key, data)
        else:
            if self.updated_by == 'manual':
                priority = 1
            else:
                priority = self.collector_priority.get(self.updated_by, _DEFAULT_PRIORITY)

            self.change_history[key] = {
                'priority': priority,
                'data': data,
                'job_id': self.job_id,
                'updated_by': self.updated_by,
                'updated_at': self.updated_at
            }

    def _create_data_history(self, resource_data):
        for key, value in resource_data.items():
            if key in ['data', 'metadata']:
                for sub_key, sub_value in value.items():
                    self._set_data_history(f'{key}.{sub_key}', sub_value)
            else:
                self._set_data_history(key, value)

    def update_pinned_keys(self, keys, collection_info):
        change_keys = self._get_all_change_keys(collection_info.get('change_history', []))

        for key in keys:
            if key not in change_keys:
                raise ERROR_NOT_ALLOW_PINNING_KEYS(key=key)

        collection_info['pinned_keys'] = keys

        return collection_info

    @staticmethod
    def _get_all_change_keys(change_history):
        change_keys = []
        for change_info in change_history:
            change_keys.append(change_info['key'])

        return change_keys

    def merge_data_by_history(self, resource_data, old_data, **kwargs):
        self.exclude_keys = kwargs.get('exclude_keys', [])
        collection_info = old_data['collection_info']
        all_collectors = collection_info.get('collectors', [])
        all_service_accounts = collection_info.get('service_accounts', [])
        all_secrets = collection_info.get('secrets', [])
        pinned_keys = collection_info.get('pinned_keys', [])
        state = collection_info['state']

        # _LOGGER.debug('------------------')
        # _LOGGER.debug(f'Update Mode: {self.update_mode}')
        # _LOGGER.debug(f'[update_request_data: data.compute] {self.merged_data.get("data", {}).get("compute")}')

        if self.updated_by != 'manual':
            if self.updated_by not in all_secrets:
                all_collectors.append(self.updated_by)
                self.is_changed = True

            if self.service_account_id and self.service_account_id not in all_service_accounts:
                all_service_accounts.append(self.service_account_id)
                self.is_changed = True

            if self.secret_id and self.secret_id not in all_secrets:
                all_secrets.append(self.secret_id)
                self.is_changed = True

            if state != 'ACTIVE':
                state = 'ACTIVE'
                self.is_changed = True

        resource_data = self._change_metadata_path(resource_data)
        resource_data = self._exclude_data_by_pinned_keys(resource_data, pinned_keys)

        self._get_collector_priority(all_collectors)
        self._create_data_history(resource_data)
        self._load_old_data_history(old_data)

        self._merge_data_from_history(old_data)

        updated_collection_info = {
            'state': state,
            'collectors': sorted(list(set(all_collectors))),
            'service_accounts': sorted(list(set(all_service_accounts))),
            'secrets': sorted(list(set(all_secrets))),
            'change_history': self._make_change_history(self.old_history),
            'pinned_keys': pinned_keys
        }

        if self.is_changed:
            self.merged_data['collection_info'] = updated_collection_info

        # Garbage Collection
        garbage_collection = old_data.get('garbage_collection', {})
        garbage_collection[self.updated_by] = self.job_id
        self.merged_data['garbage_collection'] = garbage_collection

        # _LOGGER.debug(f'[merged_data: data.compute] {self.merged_data.get("data", {}).get("compute")}')

        return self.merged_data

    def _merge_data_from_history(self, old_data):
        for key, history_info in self.change_history.items():
            new_value = history_info['data']
            new_priority = history_info['priority']
            if key in self.old_history:
                old_priority = self.old_history[key]['priority']
                old_value = self.old_history[key]['data']

                if self.update_mode == 'MERGE':
                    new_value = self._merge_old_and_new_value(old_value, new_value)
                # _LOGGER.debug(' ')
                # _LOGGER.debug(f'[_merge_data_from_history] {key}: {old_value} -> {new_value}')
                # _LOGGER.debug(f'[_merge_data_from_history] check priority: {new_priority <= old_priority and new_value != old_value}')
                # _LOGGER.debug(' ')

                if new_priority <= old_priority and new_value != old_value:
                    history_info['diff'] = self._get_history_diff(old_value, new_value)
                    self.old_history[key] = history_info
                    self._update_merge_data(key, new_value)
                    self.is_changed = True
            else:
                self.is_changed = True
                self.old_history[key] = history_info
                self._update_merge_data(key, new_value)

        for key in ['data', 'metadata']:
            if len(self.merged_data.get(key, {}).keys()) > 0:
                temp_data = old_data.get(key, {})

                # TODO: Temporary code before metadata migration
                if key == 'metadata' and 'view' in temp_data:
                    del temp_data['view']

                temp_data.update(self.merged_data[key])
                self.merged_data[key] = temp_data

    @staticmethod
    def _merge_old_and_new_value(old_value, new_value):
        if isinstance(new_value, dict) and isinstance(old_value, dict):
            new_temp_value = copy.deepcopy(old_value)
            new_temp_value.update(new_value)
            return new_temp_value
        else:
            return new_value

    @staticmethod
    def _get_history_diff(old_data, new_data):
        if isinstance(old_data, list) and isinstance(new_data, list):
            history_diff = {
                'insert': [],
                'delete': []
            }

            for value in old_data:
                if value not in new_data:
                    history_diff['delete'].append(value)

            for value in new_data:
                if value not in old_data:
                    history_diff['insert'].append(value)
        else:
            history_diff = {
                'insert': new_data,
                'delete': old_data
            }

        return history_diff

    @staticmethod
    def _exclude_data_by_pinned_keys(resource_data, pinned_keys):
        for key in pinned_keys:
            if key.startswith('data.'):
                key_path, sub_key = key.split('.', 1)
                resource_data['data'] = resource_data.get('data', {})
                if sub_key in resource_data['data']:
                    del resource_data['data'][sub_key]

            elif key in resource_data:
                del resource_data[key]

        return resource_data

    def _update_merge_data(self, key, value):
        if key.startswith('data.'):
            key_path, sub_key = key.split('.', 1)
            self.merged_data['data'] = self.merged_data.get('data', {})
            self.merged_data['data'][sub_key] = value
        elif key.startswith('metadata.'):
            key_path, sub_key = key.split('.', 1)
            self.merged_data['metadata'] = self.merged_data.get('metadata', {})
            self.merged_data['metadata'][sub_key] = value
        else:
            self.merged_data[key] = value

    def _get_collector_priority(self, collectors):
        query = {
            'only': ['collector_id', 'priority'],
            'filter': [{
                'k': 'collector_id',
                'v': collectors,
                'o': 'in'
            }]
        }

        collector_vos, total_count = self.collector_mgr.list_collectors(query)

        for collector_vo in collector_vos:
            self.collector_priority[collector_vo.collector_id] = collector_vo.priority

    def _load_old_data_history(self, old_data):
        change_history = old_data['collection_info'].get('change_history', [])
        for change_info in change_history:
            updated_by = change_info['updated_by']
            key = change_info['key']
            self.old_history[key] = {
                'priority': self.collector_priority.get(updated_by, _DEFAULT_PRIORITY),
                'data': self._get_data_from_history_key(old_data, key),
                'diff': change_info.get('diff', {}),
                'job_id': change_info.get('job_id'),
                'updated_by': updated_by,
                'updated_at': change_info['updated_at']
            }

    @staticmethod
    def _get_data_from_history_key(data, key):
        if '.' in key:
            key_path, sub_key = key.rsplit('.', 1)
            sub_data = utils.get_dict_value(data, key_path)
            if isinstance(sub_data, dict):
                if sub_key in sub_data:
                    return sub_data.get(sub_key)
            elif isinstance(data, list):
                for value in sub_data:
                    if value.get('name') == sub_key:
                        return value
            else:
                return None
        else:
            return data.get(key)

    @staticmethod
    def _make_change_history(change_history):
        history_output = []

        for key, history_info in change_history.items():
            history_output.append({
                'key': key,
                'diff': history_info.get('diff', {}),
                'job_id': history_info.get('job_id'),
                'updated_by': history_info['updated_by'],
                'updated_at': history_info['updated_at']
            })

        return history_output
