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

    def delete_tags_by_tag_type(self, cloud_service_vo, new_tags, tag_type):
        for tag in cloud_service_vo.tags:
            if tag['type'] == 'PROVIDER':
                provider = tag['provider']
            else:
                provider = 'CUSTOM'

            if tag['type'] == tag_type and tag['key'] in [new_tag['key'] for new_tag in new_tags]:
                self._delete_cloud_svc_tag_by_vo(
                    self._get_cloud_svc_tag(cloud_service_id=cloud_service_vo.cloud_service_id,
                                            key=tag['key'], provider=provider, domain_id=cloud_service_vo.domain_id))

    def create_cloud_svc_tags(self, cloud_service_vo, new_tags):
        for tag in new_tags:
            params = {
                'cloud_service_id': cloud_service_vo.cloud_service_id,
                'key': tag['key'],
                'value': tag['value'],
                'project_id': cloud_service_vo.project_id,
                'domain_id': cloud_service_vo.domain_id
            }
            if self.provider == 'COLLECTOR':
                params.update({'provider': cloud_service_vo.provider})
            else:
                params.update({'provider': 'CUSTOM'})
            self._create_cloud_svc_tag(params)

    def create_cloud_svc_tags_by_cloud_svc_vo(self, cloud_service_vo: CloudService):
        dot_tags = cloud_service_vo.tags
        for tag in dot_tags:
            params = {
                'cloud_service_id': cloud_service_vo.cloud_service_id,
                'key': tag.key,
                'value': tag.value,
                'project_id': cloud_service_vo.project_id,
                'domain_id': cloud_service_vo.domain_id
            }
            if self.provider == 'COLLECTOR':
                params.update({'provider': cloud_service_vo.provider})
            else:
                params.update({'provider': 'CUSTOM'})
            self.cloud_svc_tag_model.create(params)

    def list_cloud_svc_tags(self, query=None):
        if query is None:
            query = {}
        return self.cloud_svc_tag_model.query(**query)

    def stat_cloud_svc_tags(self, query):
        return self.cloud_svc_tag_model.stat(**query)

    def filter_cloud_svc_tags(self, **conditions):
        return self.cloud_svc_tag_model.filter(**conditions)

    def _create_cloud_svc_tag(self, params):
        def _rollback(cloud_svc_tag_vo):
            _LOGGER.info(
                f'[ROLLBACK] Delete Cloud Service Tag : {cloud_svc_tag_vo.key} ({cloud_svc_tag_vo.cloud_service_id})')
            cloud_svc_tag_vo.delete(True)

        cloud_svc_tag_vo: CloudServiceTag = self.cloud_svc_tag_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_tag_vo)

    def _get_cloud_svc_tag(self, cloud_service_id, key, provider, domain_id):
        return self.cloud_svc_tag_model.get(cloud_service_id=cloud_service_id, key=key, provider=provider,
                                            domain_id=domain_id)

    @staticmethod
    def _delete_cloud_svc_tag_by_vo(cloud_svc_tag_vo: CloudServiceTag):
        cloud_svc_tag_vo.delete()
