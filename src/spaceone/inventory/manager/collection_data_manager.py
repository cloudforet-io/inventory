import logging
from datetime import datetime
from jsondiff import diff

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
            'collectors': all_collectors,
            'service_accounts': all_service_accounts,
            'secrets': all_secrets,
            'change_history': self._make_change_history(self.change_history),
            'collected_at': self.updated_at
        }

        return collection_info

    def _set_data_history(self, key, data):
        if key not in self.exclude_keys:
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

        if self.collector_id:
            all_collectors.append(self.collector_id)

            if self.service_account_id:
                all_service_accounts.append(self.service_account_id)

            if self.secret_id:
                all_secrets.append(self.secret_id)

            state = 'ACTIVE'
        else:
            self.collector_id = 'MANUAL'
            state = collection_info['state']

        for key in pinned_keys:
            resource_data = self._update_data_by_key(resource_data, key, action='exclude')

        self._get_collector_priority(all_collectors)
        self._create_data_history(resource_data)
        self._load_old_data_history(old_data)

        merged_data = self._merge_data(old_data)

        if 'metadata' in resource_data:
            merged_data['metadata'] = resource_data['metadata']

        merged_data['collection_info'] = {
            'state': state,
            'collectors': list(set(all_collectors)),
            'service_accounts': list(set(all_service_accounts)),
            'secrets': list(set(all_secrets)),
            'change_history': self._make_change_history(self.old_history),
            'pinned_keys': pinned_keys,
            'collected_at': self.updated_at
        }

        return merged_data

    def _merge_data(self, merged_data):
        for key, history_info in self.change_history.items():
            new_data = history_info['data']
            new_priority = history_info['priority']
            if key in self.old_history:
                old_priority = self.old_history[key]['priority']
                old_data = self.old_history[key]['data']
                if new_priority <= old_priority and new_data != old_data:
                    history_info['diff'] = self._get_history_diff(old_data, new_data)
                    self.old_history[key] = history_info
                    self._update_data_by_key(merged_data, key, value=new_data)
            else:
                self.old_history[key] = history_info
                self._update_data_by_key(merged_data, key, value=new_data)

        return merged_data

    @staticmethod
    def _get_history_diff(old_data, new_data):
        history_diff = {}
        try:
            if isinstance(old_data, list) and isinstance(new_data, list):
                for value in old_data:
                    if value not in new_data:
                        if '$delete' not in history_diff:
                            history_diff['$delete'] = []

                        history_diff['$delete'].append(value)

                for value in new_data:
                    if value not in old_data:
                        if '$insert' not in history_diff:
                            history_diff['$insert'] = []

                        history_diff['$insert'].append(value)
            else:
                history_diff = utils.load_json(
                    diff(old_data, new_data, syntax='symmetric', dump=True))
        except Exception:
            history_diff = utils.load_json(
                diff(str(old_data), str(new_data), syntax='symmetric', dump=True))

        result = {
            'insert': [],
            'delete': [],
            'update': {}
        }

        if '$insert' in history_diff:
            result['insert'] = history_diff['$insert']
            del history_diff['$insert']

        if '$delete' in history_diff:
            result['delete'].append(history_diff['$delete'])
            del history_diff['$delete']

        _LOGGER.debug(f'[_get_history_diff] {str(history_diff)}')
        result['update'] = history_diff
        return result

    @staticmethod
    def _update_data_by_key(resource_data, key, action='set', value=None):
        if key.startswith('data.'):
            key_path, sub_key = key.split('.', 1)
            resource_data['data'] = resource_data.get('data', {})
            if action == 'set':
                resource_data['data'][sub_key] = value
            elif action == 'exclude':
                if sub_key in resource_data['data']:
                    del resource_data['data'][sub_key]

        elif key in resource_data:
            if action == 'set':
                resource_data[key] = value
            elif action == 'exclude':
                del resource_data[key]

        return resource_data

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
