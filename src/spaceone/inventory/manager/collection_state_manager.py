import logging

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.collection_state_model import CollectionState
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)


class CollectionStateManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collector_id = self.transaction.get_meta('collector_id')
        self.job_task_id = self.transaction.get_meta('job_task_id')
        self.collection_state_model: CollectionState = self.locator.get_model('CollectionState')

    def create_collection_state(self, resource_id, domain_id):
        if self.collector_id and self.job_task_id:
            state_data = {
                'collector_id': self.collector_id,
                'job_task_id': self.job_task_id,
                'resource_id': resource_id,
                'domain_id': domain_id
            }
            self.collection_state_model.create(state_data)

    def delete_collection_state_by_resource_id(self, resource_id, domain_id):
        state_vos = self.collection_state_model.filter(resource_id=resource_id, domain_id=domain_id)
        state_vos.delete()

    def delete_collection_state_by_resource_ids(self, resource_ids):
        _filter = [
            {
                'k': 'resource_id',
                'v': resource_ids,
                'o': 'in'
            }
        ]
        state_vos, total_count = self.collection_state_model.query(filter=_filter)
        state_vos.delete()

    def delete_collection_state_by_collector_id(self, collector_id, domain_id):
        state_vos = self.collection_state_model.filter(collector_id=collector_id, domain_id=domain_id)
        state_vos.delete()
