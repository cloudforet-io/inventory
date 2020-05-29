import logging

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.lib.resource_manager import ResourceManager


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
        return cloud_svc_type_vo.update(params)

    def delete_cloud_service_type(self, cloud_service_type_id, domain_id):
        self.delete_cloud_service_type_by_vo(self.get_cloud_service_type(cloud_service_type_id, domain_id))

    def get_cloud_service_type(self, cloud_service_type_id, domain_id, only=None):
        return self.cloud_svc_type_model.get(cloud_service_type_id=cloud_service_type_id, domain_id=domain_id,
                                             only=only)

    def list_cloud_service_types(self, query, include_cloud_service_count=False):
        cloud_svc_type_vos, total_count = self.cloud_svc_type_model.query(**query)

        if include_cloud_service_count:
            cloud_svc_model: CloudService = self.locator.get_model('CloudService')

            _filter_groups = []
            _filter_provider = []
            _filter_name = []

            for cloud_svc_type_vo in cloud_svc_type_vos:
                _filter_groups.append(cloud_svc_type_vo.group)
                _filter_provider.append(cloud_svc_type_vo.provider)
                _filter_name.append(cloud_svc_type_vo.name)

            cloud_svc_filter = [{
                'k': 'cloud_service_group',
                'v': list(set(_filter_groups)),
                'o': 'in'
            },{
                'k': 'provider',
                'v': list(set(_filter_provider)),
                'o': 'in'
            },{
                'k': 'cloud_service_type',
                'v': list(set(_filter_name)),
                'o': 'in'
            }]

            # Get domain_id from query
            for _f in query.get('filter', []):
                if _f.get('k', None) == 'domain_id' or _f.get('key', None) == 'domain_id':
                    cloud_svc_filter.append(_f)

            cloud_svc_vos, cloud_svc_total_count = cloud_svc_model.query(filter=cloud_svc_filter)

            for cloud_svc_type_vo in cloud_svc_type_vos:
                svc_count = self._get_cloud_service_count_in_type(cloud_svc_type_vo, cloud_svc_vos)
                setattr(cloud_svc_type_vo, 'cloud_service_count', svc_count)

        return cloud_svc_type_vos, total_count

    def stat_cloud_service_types(self, query):
        return self.cloud_svc_type_model.stat(**query)

    @staticmethod
    def delete_cloud_service_type_by_vo(cloud_svc_type_vo):
        cloud_svc_type_vo.delete()

    def _get_cloud_service_count_in_type(self, cloud_svc_type_vo, cloud_svc_vos):
        count = 0
        for cloud_svc_vo in cloud_svc_vos:
            if cloud_svc_type_vo.group == cloud_svc_vo.cloud_service_group and \
                    cloud_svc_type_vo.provider == cloud_svc_vo.provider and \
                    cloud_svc_type_vo.name == cloud_svc_vo.cloud_service_type:

                count = count + 1

        return count
