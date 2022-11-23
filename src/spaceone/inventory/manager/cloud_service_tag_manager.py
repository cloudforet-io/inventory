import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_tag_model import CloudServiceTag

_LOGGER = logging.getLogger(__name__)


class CloudServiceTagManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_tag_model: CloudServiceTag = self.locator.get_model('CloudServiceTag')
        self.collector_id = self.transaction.get_meta('collector_id')
        self.job_id = self.transaction.get_meta('job_id')
        self.plugin_id = self.transaction.get_meta('plugin_id')
        self.service_account_id = self.transaction.get_meta('secret.service_account_id')
        if self.collector_id and self.job_id and self.service_account_id and self.plugin_id:
            self.tag_type = 'PROVIDER'
        else:
            self.tag_type = 'CUSTOM'

    def create_cloud_svc_tag(self, params):
        def _rollback(cloud_svc_tag_vo):
            _LOGGER.info(
                f'[ROLLBACK] Delete Cloud Service Tag : {cloud_svc_tag_vo.key} ({cloud_svc_tag_vo.cloud_service_id})')
            cloud_svc_tag_vo.delete(True)

        cloud_svc_tag_vo: CloudServiceTag = self.cloud_svc_tag_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_tag_vo)

    def delete_tags_by_tag_type(self, domain_id, cloud_service_id, tag_type):
        cloud_svc_tag_vos = self.filter_cloud_svc_tags(domain_id=domain_id, cloud_service_id=cloud_service_id,
                                                       type=tag_type)
        cloud_svc_tag_vos.delete()

    def create_cloud_svc_tags_by_new_tags(self, cloud_service_vo, new_tags):
        for tag in new_tags:
            params = {
                'cloud_service_id': cloud_service_vo.cloud_service_id,
                'key': tag['key'],
                'value': tag['value'],
                'type': tag['type'],
                'provider': tag['provider'],
                'domain_id': cloud_service_vo.domain_id
            }
            self.create_cloud_svc_tag(params)

    def list_cloud_svc_tags(self, query=None):
        if query is None:
            query = {}
        return self.cloud_svc_tag_model.query(**query)

    def stat_cloud_svc_tags(self, query):
        return self.cloud_svc_tag_model.stat(**query)

    def filter_cloud_svc_tags(self, **conditions):
        return self.cloud_svc_tag_model.filter(**conditions)
