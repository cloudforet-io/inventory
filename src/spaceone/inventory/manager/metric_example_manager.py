import logging
from typing import Tuple

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.metric_example.database import MetricExample

_LOGGER = logging.getLogger(__name__)


class MetricExampleManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metric_example_model = MetricExample

    def create_metric_example(self, params: dict) -> MetricExample:
        def _rollback(vo: MetricExample):
            _LOGGER.info(
                f"[create_metric_example._rollback] "
                f"Delete metric_example: {vo.example_id}"
            )
            vo.delete()

        metric_example_vo: MetricExample = self.metric_example_model.create(params)
        self.transaction.add_rollback(_rollback, metric_example_vo)

        return metric_example_vo

    def update_metric_example_by_vo(
        self, params: dict, metric_example_vo: MetricExample
    ) -> MetricExample:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_metric_example_by_vo._rollback] Revert Data: "
                f'{old_data["example_id"]}'
            )
            metric_example_vo.update(old_data)

        self.transaction.add_rollback(_rollback, metric_example_vo.to_dict())

        return metric_example_vo.update(params)

    @staticmethod
    def delete_metric_example_by_vo(metric_example_vo: MetricExample) -> None:
        metric_example_vo.delete()

    def get_metric_example(
        self,
        example_id: str,
        domain_id: str,
        user_id: str,
        workspace_id: str = None,
    ) -> MetricExample:
        conditions = {
            "example_id": example_id,
            "domain_id": domain_id,
            "user_id": user_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        return self.metric_example_model.get(**conditions)

    def filter_metric_examples(self, **conditions) -> QuerySet:
        return self.metric_example_model.filter(**conditions)

    def list_metric_examples(self, query: dict) -> Tuple[QuerySet, int]:
        return self.metric_example_model.query(**query)

    def stat_metric_examples(self, query: dict) -> dict:
        return self.metric_example_model.stat(**query)
