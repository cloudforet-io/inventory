import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_tag_model import CloudServiceTag

_LOGGER = logging.getLogger(__name__)


class CloudServiceTagManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_tag_model: CloudServiceTag = self.locator.get_model('CloudServiceTag')

    def create_cloud_service_tag(self, params):
        def _rollback(cloud_svc_tag_vo: CloudServiceTag):
            _LOGGER.info(
                f'[ROLLBACK] Delete Tag : {cloud_svc_tag_vo.tag_id} ({cloud_svc_tag_vo.cloud_service_id})')
            cloud_svc_tag_vo.delete()

        cloud_svc_tag_vo: CloudServiceTag = self.cloud_svc_tag_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_tag_vo)

        return cloud_svc_tag_vo

    def delete_cloud_service_tag(self, tag_id, domain_id):
        cloud_svc_tag_vo = self.get_cloud_service_tag(tag_id, domain_id)
        cloud_svc_tag_vo.delete()

    def get_cloud_service_tag(self, tag_id, domain_id, only=None):
        return self.cloud_svc_tag_model.get(tag_id=tag_id, domain_id=domain_id, only=only)

    def list_cloud_service_tags(self, query={}):
        return self.cloud_svc_tag_model.query(**query)

    def stat_cloud_service_tags(self, query):
        return self.cloud_svc_tag_model.stat(**query)
