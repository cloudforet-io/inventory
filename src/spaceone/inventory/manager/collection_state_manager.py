import logging
from typing import Union, Tuple, List

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.collection_state_model import CollectionState

_LOGGER = logging.getLogger(__name__)


class CollectionStateManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collector_id = self.transaction.get_meta("collector_id")
        self.job_task_id = self.transaction.get_meta("job_task_id")
        self.secret_id = self.transaction.get_meta("secret.secret_id")
        self.collection_state_model: CollectionState = self.locator.get_model(
            "CollectionState"
        )

    def create_collection_state(self, cloud_service_id: str, domain_id: str) -> None:
        def _rollback(vo: CollectionState):
            _LOGGER.info(
                f"[ROLLBACK] Delete collection state: cloud_service_id = {vo.cloud_service_id}, "
                f"collector_id = {vo.collector_id}"
            )
            vo.terminate()

        if self.collector_id and self.job_task_id and self.secret_id:
            state_data = {
                "collector_id": self.collector_id,
                "job_task_id": self.job_task_id,
                "secret_id": self.secret_id,
                "cloud_service_id": cloud_service_id,
                "domain_id": domain_id,
            }

            state_vo = self.collection_state_model.create(state_data)
            self.transaction.add_rollback(_rollback, state_vo)

    def update_collection_state_by_vo(
        self, params: dict, state_vo: CollectionState
    ) -> CollectionState:
        def _rollback(old_data):
            _LOGGER.info(
                f"[ROLLBACK] Revert collection state : cloud_service_id = {state_vo.cloud_service_id}, "
                f"collector_id = {state_vo.collector_id}"
            )
            state_vo.update(old_data)

        self.transaction.add_rollback(_rollback, state_vo.to_dict())
        return state_vo.update(params)

    def reset_collection_state(self, state_vo: CollectionState) -> None:
        if self.job_task_id:
            params = {"disconnected_count": 0, "job_task_id": self.job_task_id}

            self.update_collection_state_by_vo(params, state_vo)

    def get_collection_state(
        self, cloud_service_id: str, domain_id: str
    ) -> Union[CollectionState, None]:
        if self.collector_id and self.secret_id:
            state_vos = self.collection_state_model.filter(
                collector_id=self.collector_id,
                secret_id=self.secret_id,
                cloud_service_id=cloud_service_id,
                domain_id=domain_id,
            )

            if state_vos.count() > 0:
                return state_vos[0]

        return None

    def filter_collection_states(self, **conditions) -> QuerySet:
        return self.collection_state_model.filter(**conditions)

    def list_collection_states(self, query: dict) -> Tuple[QuerySet, int]:
        return self.collection_state_model.query(**query)

    def delete_collection_state_by_cloud_service_id(
        self, resource_id: str, domain_id: str
    ) -> None:
        state_vos = self.filter_collection_states(
            cloud_service_id=resource_id, domain_id=domain_id
        )
        state_vos.delete()

    def delete_collection_state_by_cloud_service_ids(
        self, cloud_service_ids: List[str]
    ) -> None:
        state_vos = self.filter_collection_states(cloud_service_id=cloud_service_ids)
        state_vos.delete()

    def delete_collection_state_by_collector_id(
        self, collector_id: str, domain_id: str
    ) -> None:
        state_vos = self.filter_collection_states(
            collector_id=collector_id, domain_id=domain_id
        )
        state_vos.delete()
