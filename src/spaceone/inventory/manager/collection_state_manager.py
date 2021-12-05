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
        self.secret_id = self.transaction.get_meta('secret.secret_id')
        self.collection_state_model: CollectionState = self.locator.get_model('CollectionState')

    def create_collection_state(self, resource_id, resource_type, domain_id):
        def _rollback(state_vo: CollectionState):
            _LOGGER.info(f'[ROLLBACK] Delete Collection State : resource_id = {state_vo.resource_id}, '
                         f'collector_id = {state_vo.collector_id}')
            state_vo.terminate()

        if self.collector_id and self.job_task_id and self.secret_id:
            state_data = {
                'collector_id': self.collector_id,
                'job_task_id': self.job_task_id,
                'secret_id': self.secret_id,
                'resource_id': resource_id,
                'resource_type': resource_type,
                'domain_id': domain_id
            }

            _LOGGER.debug(f'[create_collection_state] create collection state: {state_data}')
            state_vo = self.collection_state_model.create(state_data)
            self.transaction.add_rollback(_rollback, state_vo)

    def update_collection_state_by_vo(self, params, state_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Collection State : resource_id={state_vo.resource_id}, '
                         f'collector_id={state_vo.collector_id}')
            state_vo.update(old_data)

        self.transaction.add_rollback(_rollback, state_vo.to_dict())
        return state_vo.update(params)

    def reset_collection_state(self, state_vo):
        if self.job_task_id:
            params = {
                'disconnected_count': 0,
                'job_task_id': self.job_task_id
            }

            self.update_collection_state_by_vo(params, state_vo)

    def get_collection_state(self, resource_id, domain_id):
        if self.collector_id and self.secret_id:
            state_vos = self.collection_state_model.filter(collector_id=self.collector_id, secret_id=self.secret_id,
                                                           resource_id=resource_id, domain_id=domain_id)

            if state_vos.count() > 0:
                return state_vos[0]

        return None

    def filter_collection_states(self, **conditions):
        return self.collection_state_model.filter(**conditions)

    def list_collection_states(self, query):
        return self.collection_state_model.query(**query)

    def delete_collection_state_by_resource_id(self, resource_id, domain_id):
        _LOGGER.debug(f'[delete_collection_state_by_resource_id] delete collection state: {resource_id}')
        state_vos = self.collection_state_model.filter(resource_id=resource_id, domain_id=domain_id)
        state_vos.delete()

    def delete_collection_state_by_resource_ids(self, resource_ids):
        _LOGGER.debug(f'[delete_collection_state_by_resource_ids] delete collection state: {resource_ids}')
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
        _LOGGER.debug(f'[delete_collection_state_by_collector_id] delete collection state: {collector_id}')
        state_vos = self.collection_state_model.filter(collector_id=collector_id, domain_id=domain_id)
        state_vos.delete()
