import json
import logging

from spaceone.core.scheduler.worker import BaseWorker
from spaceone.core.locator import Locator
from spaceone.core.transaction import Transaction
from spaceone.core import queue

from spaceone.inventory.manager.collector_manager.collecting_manager import CollectingManager

_LOGGER = logging.getLogger(__name__)

class InventoryDBUpdater(BaseWorker):
    def __init__(self, queue, **kwargs):
        BaseWorker.__init__(self, queue, **kwargs)
        self.locator = Locator()


    def run(self):
        """ Infinite Loop
        """
        # Create Manager
        collecting_mgr = self.locator.get_manager('CollectingManager')

        while True:
            # Read from Queue
            try:
                binary_resource_info = queue.get(self.queue)
                resource_info = json.loads(binary_resource_info.decode())
                # Create Transaction
                collecting_mgr.transaction = Transaction(resource_info['meta'])
                # processing
                method = resource_info['method']
                if method == '_process_single_result':
                    collecting_mgr._process_single_result(resource_info['res'], resource_info['param'])
                elif method == '_watchdog_job_task_stat':
                    collecting_mgr._watchdog_job_task_stat(resource_info['param'])
                else:
                    _LOGGER.error(f'Unknown request: {resource_info}')

            except Exception as e:
                _LOGGER.error(f'[{self._name_}] failed to processing: {e}')
                continue

