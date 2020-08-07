# -*- coding: utf-8 -*-

import abc
import logging

from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager
from spaceone.inventory.model.task_item_model import TaskItem
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)


class TaskItemManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_item_model: TaskItem = self.locator.get_model('TaskItem')

    def create_item_by_resource_vo(self, manager, resource_vo, resource_type, state, job_id, job_task_id, domain_id):
        # guess resource_id by manager
        resource_id, references = self._get_resource_name(resource_type, resource_vo, manager, domain_id)

        return self.create_item(resource_id, resource_type, references, state, job_id, job_task_id, domain_id)

    def create_item(self, resource_id, resource_type, references, state, job_id, job_task_id, domain_id):
        def _rollback(task_item_vo):
            _LOGGER.info(f'[ROLLBACK] Delete item: {task_item_vo.resource_id}, {task_item_vo.job_id}')
            task_item_vo.delete()

        params = {
            'resource_id': resource_id,
            'resource_type': resource_type,
            'references': references,
            'state': state,
            'job_id': job_id,
            'job_task_id': job_task_id,
            'domain_id': domain_id
        }
        task_item_vo: TaskItem = self.task_item_model.create(params)

        self.transaction.add_rollback(_rollback, task_item_vo)

        return task_item_vo

    def get(self, resource_id, job_id, domain_id):
        return self.task_item_model.get(resource_id=resource_id, job_id=job_id, domain_id=domain_id)

    def delete(self, resource_id, job_id, domain_id):
        task_item_vo = self.get(resource_id, job_id, domain_id)
        task_item_vo.delete()

    def list(self, query):
        return self.task_item_model.query(**query)

    def stat(self, query):
        return self.task_item_model.stat(**query)

    def _get_resource_name(self, resource_type, resource_vo, manager, domain_id):
        resource_keys = manager.resource_keys
        if resource_keys is None:
            raise ERROR_RESOURCE_KEYS_NOT_DEFINED(resource_type=resource_type)

        for key in resource_keys:
            if hasattr(resource_vo, key):
                resource_id = getattr(resource_vo, key)
                resource_type_sub = resource_type.split('.')[1]
                srn = f'srn://inventory/{domain_id}/{resource_type_sub}/{key}:{resource_id}'
                return resource_id, [srn]
        _LOGGER.error(f'[_get_resource_id] can not find {resource_type} {resource_vo}')
        raise ERROR_RESOURCE_KEYS_NOT_DEFINED(resource_type=resource_type)
