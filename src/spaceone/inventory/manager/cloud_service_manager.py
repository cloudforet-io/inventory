import logging

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.lib.resource_manager import ResourceManager

_LOGGER = logging.getLogger(__name__)


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
        return cloud_svc_vo.update(params)

    def delete_cloud_service(self, cloud_service_id, domain_id):
        self.delete_cloud_service_by_vo(self.get_cloud_service(cloud_service_id, domain_id))

    def get_cloud_service(self, cloud_service_id, domain_id, only=None):
        return self.cloud_svc_model.get(cloud_service_id=cloud_service_id, domain_id=domain_id, only=only)

    def list_cloud_services(self, query):
        # Append Query for DELETED filter (Temporary Logic)
        query = self._append_state_query(query)
        return self.cloud_svc_model.query(**query)

    def stat_cloud_services(self, query):
        # Append Query for DELETED filter (Temporary Logic)
        query = self._append_state_query(query)
        return self.cloud_svc_model.stat(**query)

    @staticmethod
    def delete_cloud_service_by_vo(cloud_svc_vo):
        cloud_svc_vo.delete()

    @staticmethod
    def _append_state_query(query):
        state_default_filter = {
            'key': 'state',
            'value': 'DELETED',
            'operator': 'not'
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
