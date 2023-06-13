"""
DEPRECATED (NOT USED)
"""
import json
import logging
from spaceone.core.scheduler.worker import BaseWorker
from spaceone.core.locator import Locator
from spaceone.core.transaction import Transaction
from spaceone.core import queue
from spaceone.inventory.manager.collecting_manager import CollectingManager


_LOGGER = logging.getLogger(__name__)

class InventoryDBUpdater(BaseWorker):
    def __init__(self, queue, **kwargs):
        BaseWorker.__init__(self, queue, **kwargs)
        self.locator = Locator()

    def run(self):
        collecting_mgr: CollectingManager = self.locator.get_manager(CollectingManager)

        while True:
            try:
                resource_info = json.loads(queue.get(self.queue).decode())
                collecting_mgr.transaction = Transaction(resource_info['meta'])

                method = resource_info['method']
                if method == 'check_resource_state':
                    collecting_mgr.check_resource_state(resource_info['res'], resource_info['param'])
                elif method == 'watchdog_job_task_stat':
                    collecting_mgr.watchdog_job_task_stat(resource_info['param'])
                else:
                    _LOGGER.error(f'Unknown request: {resource_info}')

            except Exception as e:
                _LOGGER.error(f'[{self._name_}] failed to processing: {e}')
                continue

