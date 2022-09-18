import logging

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_model import CloudService
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
            self.provider = 'COLLECTOR'
        else:
            self.provider = 'CUSTOM'

    def create_cloud_svc_tags_by_cloud_svc_vo(self, cloud_service_vo: CloudService, update_cloud_svc_tags=None):
        if update_cloud_svc_tags:
            dot_tags = cloud_service_vo.tags[-len(update_cloud_svc_tags):]
        else:
            dot_tags = cloud_service_vo.tags
        for tag in dot_tags:
            params = {
                'cloud_service_id': cloud_service_vo.cloud_service_id,
                'k': tag.k,
                'v': tag.v,
                'project_id': cloud_service_vo.project_id,
                'domain_id': cloud_service_vo.domain_id
            }
            if self.provider == 'COLLECTOR':
                params.update({'provider': cloud_service_vo.provider})
            else:
                params.update({'provider': 'CUSTOM'})
            self.cloud_svc_tag_model.create(params)

    def delete_cloud_svc_tags_by_cloud_service_id(self, cloud_service_id, domain_id):
        _LOGGER.debug(
            f'[delete_cloud_service_tags_by_cloud_service_id] delete cloud_service_tags: {cloud_service_id}')
        cloud_svc_tag_vos = self.cloud_svc_tag_model.filter(cloud_service_id=cloud_service_id, domain_id=domain_id)
        cloud_svc_tag_vos.delete()

    def get_cloud_svc_tag(self, k, provider, domain_id):
        return self.cloud_svc_tag_model.get(k=k, provider=provider, domain_id=domain_id)

    def delete_cloud_svc_tag(self, delete_cloud_svc_tag, domain_id):
        key = delete_cloud_svc_tag['k']
        if delete_cloud_svc_tag['provider']:
            provider = delete_cloud_svc_tag['provider']
        else:
            provider = 'CUSTOM'
        self.delete_cloud_svc_tag_by_vo(
            self.get_cloud_svc_tag(k=key, provider=provider, domain_id=domain_id))

    def list_cloud_svc_tags(self, query=None):
        if query is None:
            query = {}
        return self.cloud_svc_tag_model.query(**query)

    def stat_cloud_svc_tags(self, query):
        return self.cloud_svc_tag_model.stat(**query)

    @staticmethod
    def delete_cloud_svc_tag_by_vo(cloud_svc_tag_vo: CloudServiceTag):
        cloud_svc_tag_vo.delete()
