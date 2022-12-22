import logging
import copy
from datetime import datetime

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.lib.resource_manager import ResourceManager
from spaceone.inventory.manager.collection_state_manager import CollectionStateManager

_LOGGER = logging.getLogger(__name__)

MERGE_KEYS = [
    'name',
    'account',
    'instance_type',
    'instance_size',
    'ip_addresses',
    'reference'
    'region_code',
    'ref_region',
    'project_id'
    'collection_info',
    'tags',
    'data',
    'metadata',
]


class CloudServiceManager(BaseManager, ResourceManager):

    resource_keys = ['cloud_service_id']
    query_method = 'list_cloud_services'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_model: CloudService = self.locator.get_model('CloudService')

    def create_cloud_service(self, params):
        def _rollback(cloud_svc_vo):
            _LOGGER.info(
                f'[ROLLBACK] Delete Cloud Service : {cloud_svc_vo.provider} ({cloud_svc_vo.cloud_service_type})')
            cloud_svc_vo.delete(True)

        cloud_svc_vo: CloudService = self.cloud_svc_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_vo)

        return cloud_svc_vo

    def update_cloud_service(self, params):
        return self.update_cloud_service_by_vo(params,
                                               self.get_cloud_service(params['cloud_service_id'], params['domain_id']))

    def update_cloud_service_by_vo(self, params, cloud_svc_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("cloud_service_id")}')
            cloud_svc_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cloud_svc_vo.to_dict())
        cloud_svc_vo: CloudService = cloud_svc_vo.update(params)

        return cloud_svc_vo

    def delete_cloud_service(self, cloud_service_id, domain_id):
        cloud_svc_vo = self.get_cloud_service(cloud_service_id, domain_id)
        cloud_svc_vo.delete()

    @staticmethod
    def delete_cloud_service_by_vo(cloud_svc_vo):
        cloud_svc_vo.delete()

    def get_cloud_service(self, cloud_service_id, domain_id, user_projects=None, only=None):
        kwargs = {
            'cloud_service_id': cloud_service_id,
            'domain_id': domain_id
        }

        if user_projects:
            kwargs['project_id'] = user_projects

        if only:
            kwargs['only'] = only

        return self.cloud_svc_model.get(**kwargs)

    def list_cloud_services(self, query, target=None):
        # Append Query for DELETED filter (Temporary Logic)
        query = self._append_state_query(query)
        return self.cloud_svc_model.query(**query, target=target)

    def stat_cloud_services(self, query):
        # Append Query for DELETED filter (Temporary Logic)
        query = self._append_state_query(query)
        return self.cloud_svc_model.stat(**query)

    @staticmethod
    def merge_data(new_data, old_data):
        for key in MERGE_KEYS:
            if key in new_data:
                new_value = new_data[key]
                old_value = old_data.get(key)
                if key in ['data', 'metadata']:
                    is_changed = False
                    for sub_key, sub_value in new_value.items():
                        if sub_value != old_value.get(sub_key):
                            is_changed = True
                            break

                    if is_changed:
                        merged_value = copy.deepcopy(old_value)
                        merged_value.update(new_value)
                        new_data[key] = merged_value
                    else:
                        del new_data[key]
                else:
                    if new_value == old_value:
                        del new_data[key]

        return new_data

    def find_resources(self, query):
        query['only'] = ['cloud_service_id']

        resources = []
        cloud_svc_vos, total_count = self.list_cloud_services(query, target='SECONDARY_PREFERRED')

        for cloud_svc_vo in cloud_svc_vos:
            resources.append({
                'cloud_service_id': cloud_svc_vo.cloud_service_id
            })

        return resources, total_count

    def delete_resources(self, query):
        query['only'] = self.resource_keys

        vos, total_count = self.list_cloud_services(query)

        resource_ids = []
        for vo in vos:
            resource_ids.append(vo.cloud_service_id)

        vos.update({
            'state': 'DELETED',
            'deleted_at': datetime.utcnow()
        })

        state_mgr: CollectionStateManager = self.locator.get_manager('CollectionStateManager')
        state_mgr.delete_collection_state_by_resource_ids(resource_ids)

        return total_count

    @staticmethod
    def _append_state_query(query):
        state_default_filter = {
            'key': 'state',
            'value': 'ACTIVE',
            'operator': 'eq'
        }

        show_deleted_resource = False
        for condition in query.get('filter', []):
            key = condition.get('k', condition.get('key'))
            value = condition.get('v', condition.get('value'))
            operator = condition.get('o', condition.get('operator'))

            if key == 'state':
                if operator == 'eq' and value == 'DELETED':
                    show_deleted_resource = True
                elif operator in ['in', 'contain_in'] and 'DELETED' in value:
                    show_deleted_resource = True

        if not show_deleted_resource:
            query['filter'] = query.get('filter', [])
            query['filter'].append(state_default_filter)

        return query
