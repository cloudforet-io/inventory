import logging
from datetime import datetime
from _collections import defaultdict

from spaceone.core.manager import BaseManager
from spaceone.inventory.manager.collector_manager import CollectorManager
from spaceone.inventory.error import *

_METADATA_KEYS = ['view.table.layout', 'view.sub_data.layouts']
_LOGGER = logging.getLogger(__name__)


class CollectionDataManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.update_history = {}
        self.metadata_info = {}
        self.collector_priority = {}
        self.collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')

    def update_pinned_keys(self, keys, collection_info):
        update_keys = self._get_all_update_keys(collection_info.update_history)

        for key in keys:
            if key not in update_keys:
                raise ERROR_NOT_ALLOW_PINNING_KEYS(key=key)

        return {
            'state': collection_info.state,
            'collectors': collection_info.collectors,
            'update_history': collection_info.update_history,
            'pinned_keys': keys
        }

    @staticmethod
    def exclude_data_by_pinned_keys(resource_data, collection_info):
        for key in collection_info.pinned_keys:
            if key.startswith('data.'):
                sub_key = key.split('.')[1]

                if sub_key in resource_data.get('data', {}):
                    del resource_data['data'][sub_key]
            else:
                if key in resource_data:
                    del resource_data[key]

        return resource_data

    def create_new_history(self, resource_data, domain_id, collector_id, service_account_id, secret_id, **kwargs):
        exclude_keys = kwargs.get('exclude_keys', []) + ['metadata']
        all_collectors = []
        all_service_accounts = []
        all_secrets = []

        if collector_id:
            # self.collector_mgr.get_collector(collector_id, domain_id)
            all_collectors.append(collector_id)
            state = 'ACTIVE'

            if service_account_id:
                all_service_accounts.append(service_account_id)
            if secret_id:
                all_secrets.append(secret_id)

        else:
            collector_id = 'MANUAL'
            state = 'MANUAL'

        self._create_update_data_history(resource_data, collector_id,
                                         service_account_id, secret_id, exclude_keys)

        collection_info = {
            'state': state,
            'collectors': all_collectors,
            'service_accounts': all_service_accounts,
            'secrets': all_secrets,
            'update_history': self._make_update_history()
        }

        return collection_info

    def exclude_data_by_history(self, resource_data, old_data, domain_id, collection_info,
                                collector_id, service_account_id, secret_id, **kwargs):
        exclude_keys = kwargs.get('exclude_keys', []) + ['metadata']
        current_collectors = collection_info.collectors
        current_service_accounts = collection_info.service_accounts
        current_secrets = collection_info.secrets
        all_service_accounts = []
        all_secrets = []

        if collector_id:
            try:
                new_collector_vo = self.collector_mgr.get_collector(collector_id, domain_id)
                priority = new_collector_vo.priority
            except Exception as e:
                _LOGGER.warning(f'[exclude_data_by_history] No collector : {collector_id}')
                priority = 10

            all_collectors = list(set(current_collectors + [collector_id]))

            if service_account_id:
                all_service_accounts = list(set(current_service_accounts + [service_account_id]))

            if secret_id:
                all_secrets = list(set(current_secrets + [secret_id]))

            state = 'ACTIVE'
        else:
            collector_id = 'MANUAL'
            all_collectors = current_collectors
            all_service_accounts = current_service_accounts
            all_secrets = current_secrets
            state = collection_info.state
            priority = 10

        self._get_collector_priority(current_collectors)
        self._load_update_history(collection_info.update_history)
        self._load_old_metadata(old_data.get('metadata', {}))
        excluded_resource_data = self._compare_data(resource_data,
                                                    old_data,
                                                    collector_id,
                                                    service_account_id,
                                                    secret_id,
                                                    priority,
                                                    exclude_keys)

        excluded_resource_data['collection_info'] = {
            'state': state,
            'collectors': all_collectors,
            'service_accounts': all_service_accounts,
            'secrets': all_secrets,
            'update_history': self._make_update_history(),
            'pinned_keys': collection_info.pinned_keys
        }

        return excluded_resource_data

    @staticmethod
    def merge_data(old_data, new_data):
        merged_data = old_data.copy()
        merged_data.update(new_data)
        return merged_data

    @staticmethod
    def merge_metadata(old_metadata, new_metadata):
        return new_metadata
        metadata = {}
        for meta_key in _METADATA_KEYS:
            meta_values = defaultdict(dict)
            for sequence in (old_metadata.get(meta_key, []), new_metadata.get(meta_key, [])):
                for meta_value in sequence:
                    meta_values[meta_value['name']].update(meta_value)

            metadata[meta_key] = meta_values.values()

        return metadata

    @staticmethod
    def _exclude_metadata_item(items, collector_id):
        changed_items = []
        for item in items:
            if item.get('updated_by') != collector_id:
                changed_items.append(item)

        return changed_items

    @staticmethod
    def _get_all_update_keys(update_history):
        update_keys = []
        for update_info in update_history:
            update_keys.append(update_info.key)

        return update_keys

    def _create_update_data_history(self, resource_data, collector_id,
                                    service_account_id, secret_id, exclude_keys):
        updated_at = datetime.utcnow().timestamp()

        for key, value in resource_data.items():
            if key == 'data':
                self._set_data_history(value, exclude_keys, collector_id, updated_at,
                                       service_account_id, secret_id)
            elif key == 'metadata':
                self._set_metadata_history(value, collector_id, updated_at,
                                           service_account_id, secret_id)
            else:
                self._set_field_data_history(key, exclude_keys, collector_id, updated_at,
                                             service_account_id, secret_id)

    def _set_data_history(self, data, exclude_keys, collector_id, updated_at,
                          service_account_id, secret_id):
        for sub_key in data.keys():
            if f'data.{sub_key}' not in exclude_keys:
                self.update_history[f'data.{sub_key}'] = {
                    'updated_by': collector_id,
                    'updated_at': updated_at,
                    'service_account_id': service_account_id,
                    'secret_id': secret_id
                }

    def _set_layout_history(self, layout, key_path, collector_id, updated_at,
                            service_account_id, secret_id):
        if not isinstance(layout, dict):
            raise ERROR_METADATA_DICT_TYPE(key=key_path)

        if 'name' in layout:
            name = layout['name'].strip()
            self.update_history[f'{key_path}.{name}'] = {
                'updated_by': collector_id,
                'updated_at': updated_at,
                'service_account_id': service_account_id,
                'secret_id': secret_id
            }

    def _set_view_metadata_history(self, view_meta, collector_id, updated_at,
                                   service_account_id, secret_id):
        if not isinstance(view_meta, dict):
            raise ERROR_INVALID_PARAMETER_TYPE(key='metadata.view', type='dict')

        if 'table' in view_meta:
            layout = view_meta['table'].get('layout')
            if not isinstance(layout, dict):
                raise ERROR_METADATA_DICT_TYPE(key='metadata.view.table.layout')

            self._set_layout_history(layout, 'metadata.view.table.layout', collector_id,
                                     updated_at, service_account_id, secret_id)

        elif 'sub_data' in view_meta:
            layouts = view_meta['sub_data'].get('layouts')
            if not isinstance(layouts, list):
                raise ERROR_METADATA_LIST_VALUE_TYPE(key='metadata.view.sub_data.layouts')

            for layout in layouts:
                self._set_layout_history(layout, 'metadata.view.sub_data.layouts', collector_id,
                                         updated_at, service_account_id, secret_id)

    def _set_metadata_history(self, metadata, collector_id, updated_at,
                              service_account_id, secret_id):

        if not isinstance(metadata, dict):
            raise ERROR_INVALID_PARAMETER_TYPE(key='metadata', type='dict')

        if 'view' in metadata:
            self._set_view_metadata_history(metadata['view'], collector_id, updated_at,
                                            service_account_id, secret_id)

    def _set_field_data_history(self, key, exclude_keys, collector_id, updated_at,
                                service_account_id, secret_id):
        if key not in exclude_keys:
            self.update_history[key] = {
                'updated_by': collector_id,
                'updated_at': updated_at,
                'service_account_id': service_account_id,
                'secret_id': secret_id
            }

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

    def _load_update_history(self, current_update_history):
        for history_info in current_update_history:
            self.update_history[history_info.key] = {
                'priority': self.collector_priority.get(history_info.updated_by, 100),
                'updated_by': history_info.updated_by,
                'updated_at': history_info.updated_at,
                'service_account_id': history_info.service_account_id,
                'secret_id': history_info.secret_id
            }

    def _load_old_metadata(self, old_metadata):
        for meta_key, meta_values in old_metadata.items():
            self.metadata_info[meta_key] = {}
            for value in meta_values:
                if 'name' in value:
                    self.metadata_info[meta_key][value['name'].strip()] = value

    def _compare_data(self, resource_data, old_data, collector_id,
                      service_account_id, secret_id, priority, exclude_keys):
        updated_at = datetime.utcnow().timestamp()
        changed_data = {}

        for key, value in resource_data.items():
            if key == 'data':
                changed_data['data'] = self._check_data_priority(value, old_data.get('data', {}), exclude_keys,
                                                                 collector_id, service_account_id,
                                                                 secret_id, priority, updated_at)
            # elif key == 'metadata':
            #     changed_data['metadata'] = self._check_metadata_priority(value, collector_id,
            #                                                              service_account_id, secret_id,
            #                                                              priority, updated_at)
            else:
                is_data_remove = self._check_field_data_priority(key, value, old_data.get(key, None),
                                                                 exclude_keys, collector_id,
                                                                 service_account_id, secret_id,
                                                                 priority, updated_at)
                if not is_data_remove:
                    changed_data[key] = value

        # return changed_data
        return resource_data

    def _check_priority(self, key, priority):
        if key in self.update_history and self.update_history[key]['priority'] < priority:
            return False
        else:
            return True

    def _check_data_priority(self, data, old_data, exclude_keys, collector_id,
                             service_account_id, secret_id, priority, updated_at):
        changed_data = {}
        for sub_key, sub_value in data.items():
            key = f'data.{sub_key}'
            if key in exclude_keys:
                changed_data[sub_key] = sub_value
            elif old_data.get(sub_key) and old_data[sub_key] == sub_value:
                pass
            else:
                if self._check_priority(key, priority):
                    changed_data[sub_key] = sub_value

                    self.update_history[key] = {
                        'updated_by': collector_id,
                        'updated_at': updated_at,
                        'service_account_id': service_account_id,
                        'secret_id': secret_id
                    }

        return changed_data

    def _check_layout_history(self, layout, key_path, collector_id, service_account_id, secret_id,
                              priority, updated_at):
        if not isinstance(layout, dict):
            raise ERROR_METADATA_DICT_TYPE(key=key_path)
        #
        # if 'name' in layout:
        #     name = layout['name'].strip()
        #     history_key = f'{key_path}.{name}'
        #
        #     if self.metadata_info.get(meta_key) and self.metadata_info[meta_key] == value:
        #
        #     self.update_history[f'{key_path}.{name}'] = {
        #         'updated_by': collector_id,
        #         'updated_at': updated_at,
        #         'service_account_id': service_account_id,
        #         'secret_id': secret_id
        #     }
        #
        #
        # for meta_key, meta_values in metadata.items():
        #     self._check_metadata_item(meta_key, meta_values)
        #     changed_metadata[meta_key] = []
        #     for value in meta_values:
        #         if 'name' in value:
        #             meta_name = value['name'].strip()
        #             history_key = f'metadata.{meta_key}.{meta_name}'
        #
        #             if self.metadata_info.get(meta_key) and self.metadata_info[meta_key] == value:
        #                 pass
        #             else:
        #                 if self._check_priority(history_key, priority):
        #                     changed_metadata[meta_key].append(value)
        #
        #                     self.update_history[history_key] = {
        #                         'updated_by': collector_id,
        #                         'updated_at': updated_at,
        #                         'service_account_id': service_account_id,
        #                         'secret_id': secret_id
        #                     }

    def _check_view_metadata_history(self, view_meta, collector_id, service_account_id,
                                     secret_id, priority, updated_at):
        if not isinstance(view_meta, dict):
            raise ERROR_INVALID_PARAMETER_TYPE(key='metadata.view', type='dict')

        if 'table' in view_meta:
            layout = view_meta['table'].get('layout')
            if not isinstance(layout, dict):
                raise ERROR_METADATA_DICT_TYPE(key='metadata.view.table.layout')

            view_meta['table'] = self._check_layout_history(layout, 'metadata.view.table.layout', collector_id,
                                                            service_account_id, secret_id, priority, updated_at)

        elif 'sub_data' in view_meta:
            layouts = view_meta['sub_data'].get('layouts')
            if not isinstance(layouts, list):
                raise ERROR_METADATA_LIST_VALUE_TYPE(key='metadata.view.sub_data.layouts')

            for layout in layouts:
                view_meta['sub_data'] = self._check_layout_history(layout, 'metadata.view.sub_data.layouts',
                                                                   collector_id, service_account_id, secret_id,
                                                                   priority, updated_at)
        return view_meta

    def _check_metadata_priority(self, metadata, collector_id, service_account_id,
                                 secret_id, priority, updated_at):
        changed_metadata = {}
        if 'view' in metadata:
            changed_metadata['view'] = self._check_view_metadata_history(metadata['view'], collector_id,
                                                                         service_account_id, secret_id,
                                                                         priority, updated_at)

        return changed_metadata

    def _check_field_data_priority(self, key, value, old_value, exclude_keys, collector_id,
                                   service_account_id, secret_id, priority, updated_at):
        if key in exclude_keys:
            return False
        elif value == old_value:
            return True
        else:
            if self._check_priority(key, priority):
                self.update_history[key] = {
                    'updated_by': collector_id,
                    'updated_at': updated_at,
                    'service_account_id': service_account_id,
                    'secret_id': secret_id
                }

                return False
            else:
                return True

    def _make_update_history(self):
        update_history = []

        for key, value in self.update_history.items():
            update_history.append({
                'key': key,
                'updated_by': value['updated_by'],
                'updated_at': value['updated_at'],
                'service_account_id': value.get('service_account_id'),
                'secret_id': value.get('secret_id')
            })

        return update_history
