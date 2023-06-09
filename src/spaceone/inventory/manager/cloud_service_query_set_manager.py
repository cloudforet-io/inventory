import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.error import *
from spaceone.inventory.model.cloud_service_query_set_model import CloudServiceQuerySet

_LOGGER = logging.getLogger(__name__)


class CloudServiceQuerySetManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_query_set_model: CloudServiceQuerySet = self.locator.get_model('CloudServiceQuerySet')

    def create_cloud_service_query_set(self, params):
        def _rollback(cloud_svc_query_set_vo: CloudServiceQuerySet):
            _LOGGER.info(
                f'[ROLLBACK] Delete Cloud Service Query Set : {cloud_svc_query_set_vo.name} ({cloud_svc_query_set_vo.query_set_id})')
            cloud_svc_query_set_vo.delete()

        cloud_svc_query_set_vo: CloudServiceQuerySet = self.cloud_svc_query_set_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_query_set_vo)

        return cloud_svc_query_set_vo

    def update_cloud_service_query_set(self, params):
        return self.update_cloud_service_query_set_by_vo(
            params, self.get_cloud_service_query_set(params['query_set_id'], params['domain_id']))

    def update_cloud_service_query_set_by_vo(self, params, cloud_svc_query_set_vo: CloudServiceQuerySet):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("query_set_id")}')
            cloud_svc_query_set_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cloud_svc_query_set_vo.to_dict())
        return cloud_svc_query_set_vo.update(params)

    def delete_cloud_service_query_set(self, query_set_id, domain_id):
        self.delete_cloud_service_query_set_by_vo(self.get_cloud_service_query_set(query_set_id, domain_id))

    @staticmethod
    def delete_cloud_service_query_set_by_vo(cloud_svc_query_set_vo: CloudServiceQuerySet):
        cloud_svc_query_set_vo.delete()

    def get_cloud_service_query_set(self, query_set_id, domain_id, only=None):
        return self.cloud_svc_query_set_model.get(query_set_id=query_set_id, domain_id=domain_id, only=only)

    def run_cloud_service_query_set(self, cloud_svc_query_set_vo: CloudServiceQuerySet):
        pass

    def filter_cloud_service_query_sets(self, **conditions):
        return self.cloud_svc_query_set_model.filter(**conditions)

    def list_cloud_service_query_sets(self, query):
        cloud_svc_query_set_vos, total_count = self.cloud_svc_query_set_model.query(**query)
        return cloud_svc_query_set_vos, total_count

    def stat_cloud_service_query_sets(self, query):
        return self.cloud_svc_query_set_model.stat(**query)

