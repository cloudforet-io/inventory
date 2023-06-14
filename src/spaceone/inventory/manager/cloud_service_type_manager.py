import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.lib.resource_manager import ResourceManager
from spaceone.inventory.manager.cloud_service_query_set_manager import CloudServiceQuerySetManager


_LOGGER = logging.getLogger(__name__)


class CloudServiceTypeManager(BaseManager, ResourceManager):

    resource_keys = ['cloud_service_type_id']
    query_method = 'list_cloud_service_types'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_type_model: CloudServiceType = self.locator.get_model('CloudServiceType')

    def create_cloud_service_type(self, params):
        def _rollback(cloud_svc_type_vo):
            _LOGGER.info(f'[ROLLBACK] Delete Cloud Service Type : {cloud_svc_type_vo.name} ({cloud_svc_type_vo.cloud_service_type_id})')
            cloud_svc_type_vo.delete(True)

        cloud_svc_type_vo: CloudServiceType = self.cloud_svc_type_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_type_vo)

        self._create_cloud_service_query_sets(params.get('metadata', {}), cloud_svc_type_vo)

        return cloud_svc_type_vo

    def update_cloud_service_type(self, params):
        return self.update_cloud_service_type_by_vo(params,
                                                    self.get_cloud_service_type(params['cloud_service_type_id'],
                                                                                params['domain_id']))

    def update_cloud_service_type_by_vo(self, params, cloud_svc_type_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("cloud_service_type_id")}')
            cloud_svc_type_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cloud_svc_type_vo.to_dict())

        self._update_cloud_service_query_sets(params.get('metadata', {}), cloud_svc_type_vo)

        return cloud_svc_type_vo.update(params)

    def delete_cloud_service_type(self, cloud_service_type_id, domain_id):
        self.delete_cloud_service_type_by_vo(self.get_cloud_service_type(cloud_service_type_id, domain_id))

    def get_cloud_service_type(self, cloud_service_type_id, domain_id, only=None):
        return self.cloud_svc_type_model.get(cloud_service_type_id=cloud_service_type_id, domain_id=domain_id,
                                             only=only)

    def list_cloud_service_types(self, query):
        cloud_svc_type_vos, total_count = self.cloud_svc_type_model.query(**query)
        return cloud_svc_type_vos, total_count

    def stat_cloud_service_types(self, query):
        return self.cloud_svc_type_model.stat(**query)

    def delete_cloud_service_type_by_vo(self, cloud_svc_type_vo):
        self._delete_cloud_service_query_sets(cloud_svc_type_vo)
        cloud_svc_type_vo.delete()

    def delete_resources(self, query):
        query['only'] = self.resource_keys

        cloud_service_type_vos, total_count = self.list_cloud_service_types(query)
        for cloud_service_type_vo in cloud_service_type_vos:
            self.delete_cloud_service_type_by_vo(cloud_service_type_vo)

        return total_count

    def _create_cloud_service_query_sets(self, metadata: dict, cloud_service_type_vo: CloudServiceType):
        cloud_svc_query_set_mgr: CloudServiceQuerySetManager = self.locator.get_manager('CloudServiceQuerySetManager')

        for query_set in metadata.get('query_sets', []):
            if 'name' in query_set:
                query_set['query_type'] = 'MANAGED'
                query_set['provider'] = cloud_service_type_vo.provider
                query_set['cloud_service_group'] = cloud_service_type_vo.group
                query_set['cloud_service_type'] = cloud_service_type_vo.name
                query_set['domain_id'] = cloud_service_type_vo.domain_id
                cloud_svc_query_set_mgr.create_cloud_service_query_set(query_set)

    def _update_cloud_service_query_sets(self, metadata: dict, cloud_service_type_vo: CloudServiceType):
        cloud_svc_query_set_mgr: CloudServiceQuerySetManager = self.locator.get_manager('CloudServiceQuerySetManager')

        for query_set in metadata.get('query_sets', []):
            if 'name' in query_set:
                filter_params = {
                    'name': query_set['name'],
                    'provider': cloud_service_type_vo.provider,
                    'cloud_service_group': cloud_service_type_vo.group,
                    'cloud_service_type': cloud_service_type_vo.name,
                    'domain_id': cloud_service_type_vo.domain_id
                }

                query_set_vos = cloud_svc_query_set_mgr.filter_cloud_service_query_sets(**filter_params)
                if len(query_set_vos) > 0:
                    cloud_svc_query_set_mgr.update_cloud_service_query_set_by_vo(query_set, query_set_vos[0])

                else:
                    query_set['query_type'] = 'MANAGED'
                    query_set['provider'] = cloud_service_type_vo.provider
                    query_set['cloud_service_group'] = cloud_service_type_vo.group
                    query_set['cloud_service_type'] = cloud_service_type_vo.name
                    query_set['domain_id'] = cloud_service_type_vo.domain_id
                    cloud_svc_query_set_mgr.create_cloud_service_query_set(query_set)

    def _delete_cloud_service_query_sets(self, cloud_service_type_vo: CloudServiceType):
        cloud_svc_query_set_mgr: CloudServiceQuerySetManager = self.locator.get_manager('CloudServiceQuerySetManager')
        filter_params = {
            'provider': cloud_service_type_vo.provider,
            'cloud_service_group': cloud_service_type_vo.group,
            'cloud_service_type': cloud_service_type_vo.name,
            'domain_id': cloud_service_type_vo.domain_id
        }

        query_set_vos = cloud_svc_query_set_mgr.filter_cloud_service_query_sets(**filter_params)
        for query_set_vo in query_set_vos:
            cloud_svc_query_set_mgr.delete_cloud_service_query_set_by_vo(query_set_vo)
