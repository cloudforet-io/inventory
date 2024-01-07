import logging
from datetime import datetime
from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler
from spaceone.core import config, utils
from spaceone.inventory.service.collector_service import CollectorService


__all__ = ["InventoryHourlyScheduler"]

_LOGGER = logging.getLogger(__name__)


class InventoryHourlyScheduler(HourlyScheduler):
    def __init__(self, queue, interval, minute=":00"):
        super().__init__(queue, interval, minute)
        self.locator = Locator()
        self._init_config()

    def _init_config(self):
        self._token = config.get_global("TOKEN")
        if self._token is None:
            raise ERROR_CONFIGURATION(key="TOKEN")

    def create_task(self):
        current_hour = datetime.utcnow().hour
        return [
            self._create_job_request(collector_vo)
            for collector_vo in self.list_schedule_collectors(current_hour)
        ]

    def list_schedule_collectors(self, current_hour: int):
        try:
            collector_svc: CollectorService = self.locator.get_service(
                CollectorService, {"token": self._token}
            )
            collector_vos, total_count = collector_svc.scheduled_collectors(
                {"hour": current_hour}
            )
            _LOGGER.debug(
                f"[list_schedule_collectors] scheduled collectors count (UTC {current_hour}): {total_count}"
            )
            return collector_vos
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return []

    def _create_job_request(self, collector_vo):
        schedule_job = {
            "locator": "SERVICE",
            "name": "CollectorService",
            "metadata": {
                "token": self._token,
            },
            "method": "collect",
            "params": {
                "params": {
                    "collector_id": collector_vo.collector_id,
                    "domain_id": collector_vo.domain_id,
                }
            },
        }

        _LOGGER.debug(
            f"[_create_job_request] tasks: inventory_collect_schedule: {collector_vo.collector_id}"
        )

        return {
            "name": "inventory_collect_schedule",
            "version": "v1",
            "executionEngine": "BaseWorker",
            "stages": [schedule_job],
        }
