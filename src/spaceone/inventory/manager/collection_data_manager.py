import logging
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
        self.collector_id = self.transaction.get_meta('collector_id')
        self.secret_id = self.transaction.get_meta('secret.secret_id')
        self.service_account_id = self.transaction.get_meta('secret.service_account_id')
        self.updated_at = datetime.utcnow()

    def create_new_history(self, resource_data, **kwargs):
        self.exclude_keys = kwargs.get('exclude_keys', [])
        all_collectors = []
        all_service_accounts = []
        all_secrets = []

        if self.collector_id:
            all_collectors.append(self.collector_id)
            state = 'ACTIVE'

            if self.service_account_id:
                all_service_accounts.append(self.service_account_id)

            if self.secret_id:
                all_secrets.append(self.secret_id)

        else:
            self.collector_id = 'MANUAL'
            state = 'MANUAL'

        self._create_data_history(resource_data)

        collection_info = {
            'state': state,
            'collectors': sorted(all_collectors),
            'service_accounts': sorted(all_service_accounts),
            'secrets': sorted(all_secrets),
            'change_history': self._make_change_history(self.change_history)
        }

        return collection_info

    def _set_data_history(self, key, data):
        if key in self.exclude_keys:
            self._update_merge_data(key, data)
        else:
            updated_by = self.collector_id
            if updated_by == 'MANUAL':
                priority = 1
            else:
                priority = self.collector_priority.get(updated_by, _DEFAULT_PRIORITY)

            self.change_history[key] = {
                'priority': priority,
                'data': data,
                'job_id': self.job_id,
                'updated_by': updated_by,
                'updated_at': self.updated_at
            }

    def _create_data_history(self, resource_data):
        for key, value in resource_data.items():
            if key == 'data':
                for sub_key, sub_value in value.items():
                    self._set_data_history(f'data.{sub_key}', sub_value)
            elif key == 'metadata':
                pass
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

        if self.collector_id:
            if self.collector_id not in all_secrets:
                all_collectors.append(self.collector_id)
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
        else:
            self.collector_id = 'MANUAL'

        resource_data = self._exclude_data_by_pinned_keys(resource_data, pinned_keys)

        self._get_collector_priority(all_collectors)
        self._create_data_history(resource_data)
        self._load_old_data_history(old_data)

        self._merge_data_from_history(old_data)

        if 'metadata' in resource_data and old_data['metadata'] != resource_data['metadata']:
            self.merged_data['metadata'] = resource_data['metadata']

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

        return self.merged_data

    def _merge_data_from_history(self, old_data):
        for key, history_info in self.change_history.items():
            new_value = history_info['data']
            new_priority = history_info['priority']
            if key in self.old_history:
                old_priority = self.old_history[key]['priority']
                old_value = self.old_history[key]['data']
                if new_priority <= old_priority and new_value != old_value:
                    history_info['diff'] = self._get_history_diff(old_value, new_value)
                    self.old_history[key] = history_info
                    self._update_merge_data(key, new_value)
                    self.is_changed = True
            else:
                self.is_changed = True
                self.old_history[key] = history_info
                self._update_merge_data(key, new_value)

        if len(self.merged_data.get('data', {}).keys()) > 0:
            temp_data = old_data.get('data', {})
            temp_data.update(self.merged_data['data'])
            self.merged_data['data'] = temp_data

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
                'job_id': history_info.get('job_id'),
                'diff': history_info.get('diff', {}),
                'updated_by': history_info['updated_by'],
                'updated_at': history_info['updated_at']
            })

        return history_output
