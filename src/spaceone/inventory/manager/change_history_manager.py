import logging
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.manager.record_manager import RecordManager

_LOGGER = logging.getLogger(__name__)

DIFF_KEYS = [
    'name',
    'account',
    'instance_type',
    'instance_size',
    'ip_addresses',
    'reference',
    'region_code',
    'project_id',
    'data',
    'tags',
]

MAX_KEY_DEPTH = 3


class ChangeHistoryManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.record_mgr: RecordManager = self.locator.get_manager('RecordManager')
        self.merged_data = {}
        self.is_changed = False
        self.collector_id = self.transaction.get_meta('collector_id')
        self.job_id = self.transaction.get_meta('job_id')
        self.plugin_id = self.transaction.get_meta('plugin_id')
        self.secret_id = self.transaction.get_meta('secret.secret_id')
        self.service_account_id = self.transaction.get_meta('secret.service_account_id')
        self.user_id = self.transaction.get_meta('user_id')

        if self.collector_id and self.job_id and self.service_account_id and self.plugin_id:
            self.updated_by = 'COLLECTOR'
        else:
            self.updated_by = 'USER'

    def add_new_history(self, cloud_service_vo: CloudService, new_data: dict):
        self._create_record(cloud_service_vo, new_data)

    def add_update_history(self, cloud_service_vo: CloudService, new_data: dict, old_data: dict):
        new_keys = new_data.keys()

        if len(set(new_keys) & set(DIFF_KEYS)) > 0:
            self._create_record(cloud_service_vo, new_data, old_data)

    def add_delete_history(self, cloud_service_vo: CloudService):
        params = {
            'cloud_service_id': cloud_service_vo.cloud_service_id,
            'cloud_service': cloud_service_vo,
            'domain_id': cloud_service_vo.domain_id,
            'action': 'DELETE',
        }

        self.record_mgr.create_record(params)

    def _create_record(self, cloud_service_vo: CloudService, new_data, old_data=None):
        if old_data:
            action = 'UPDATE'
        else:
            action = 'CREATE'

        diff = self._make_diff(new_data, old_data)
        diff_count = len(diff)

        if diff_count > 0:
            params = {
                'cloud_service_id': cloud_service_vo.cloud_service_id,
                'cloud_service': cloud_service_vo,
                'domain_id': cloud_service_vo.domain_id,
                'action': action,
                'diff': diff,
                'diff_count': diff_count,
                'updated_by': self.updated_by,
            }

            if self.updated_by == 'COLLECTOR':
                params['collector_id'] = self.collector_id
                params['job_id'] = self.collector_id
            else:
                params['user_id'] = self.user_id

            self.record_mgr.create_record(params)

    def _make_diff(self, new_data, old_data=None):
        diff = []
        for key in DIFF_KEYS:
            if key in new_data:
                if old_data:
                    old_value = old_data.get(key)
                else:
                    old_value = None

                diff += self._get_diff_data(key, new_data[key], old_value)

        return diff

    def _get_diff_data(self, key, new_value, old_value, depth=1, parent_key=None):
        diff = []

        if depth == MAX_KEY_DEPTH:
            if new_value != old_value:
                diff.append(self._generate_diff_data(key, parent_key, new_value, old_value))
        elif isinstance(new_value, dict):
            if parent_key:
                parent_key = f'{parent_key}.{key}'
            else:
                parent_key = key

            for sub_key, sub_value in new_value.items():
                if isinstance(old_value, dict):
                    sub_old_value = old_value.get(sub_key)
                else:
                    sub_old_value = None

                diff += self._get_diff_data(sub_key, sub_value, sub_old_value, depth+1, parent_key)
        else:
            if new_value != old_value:
                diff.append(self._generate_diff_data(key, parent_key, new_value, old_value))

        return diff

    @staticmethod
    def _generate_diff_data(key, parent_key, new_value, old_value):
        if old_value is None:
            diff_type = 'ADDED'
        else:
            diff_type = 'CHANGED'

        return {
            'key': key if parent_key is None else f'{parent_key}.{key}',
            'before': old_value,
            'after': new_value,
            'type': diff_type
        }
