from datetime import datetime
import logging
from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.core import config
from spaceone.core.locator import Locator
from spaceone.core.scheduler import HourlyScheduler
from spaceone.inventory.service.cleanup_service import CleanupService


__all__ = ["CleanupScheduler"]

_LOGGER = logging.getLogger(__name__)


INTERVAL = 10


class CleanupScheduler(HourlyScheduler):
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
            self._create_job_request(domain_info["domain_id"])
            for domain_info in self.list_domains(current_hour)
        ]

    def list_domains(self, current_hour: int):
        try:
            cleanup_svc: CleanupService = self.locator.get_service(
                CleanupService, {"token": self._token}
            )
            response = cleanup_svc.list_domains({})
            total_count = response.get("total_count", 0)

            _LOGGER.debug(
                f"[list_domains] total domain count (UTC {current_hour}): {total_count}"
            )
            return response.get("results", [])

        except Exception as e:
            _LOGGER.error(e)
            return []

    def _create_job_request(self, domain_id):
        update_job_state = {
            "locator": "SERVICE",
            "name": "CleanupService",
            "metadata": {
                "token": self._token,
            },
            "method": "update_job_state",
            "params": {
                "params": {
                    "domain_id": domain_id,
                }
            },
        }

        terminate_jobs = {
            "locator": "SERVICE",
            "name": "CleanupService",
            "metadata": {
                "token": self._token,
            },
            "method": "terminate_jobs",
            "params": {
                "params": {
                    "domain_id": domain_id,
                }
            },
        }

        delete_resources = {
            "locator": "SERVICE",
            "name": "CleanupService",
            "metadata": {
                "token": self._token,
            },
            "method": "delete_resources",
            "params": {
                "params": {
                    "domain_id": domain_id,
                }
            },
        }

        terminate_resources = {
            "locator": "SERVICE",
            "name": "CleanupService",
            "metadata": {
                "token": self._token,
            },
            "method": "terminate_resources",
            "params": {
                "params": {
                    "domain_id": domain_id,
                }
            },
        }

        stp = {
            "name": "inventory_cleanup_schedule",
            "version": "v1",
            "executionEngine": "BaseWorker",
            "stages": [
                update_job_state,
                delete_resources,
                terminate_jobs,
                terminate_resources,
            ],
        }

        _LOGGER.debug(
            f"[_create_job_request] tasks: inventory_cleanup_schedule: {domain_id}"
        )
        return stp
