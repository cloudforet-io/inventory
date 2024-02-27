import logging
from spaceone.core.service import *
from spaceone.core.service.utils import *
from spaceone.inventory.plugin.collector.model.job_request import JobGetTaskRequest
from spaceone.inventory.plugin.collector.model.job_response import TasksResponse

_LOGGER = logging.getLogger(__name__)


class JobService(BaseService):
    resource = "Job"

    @transaction
    @convert_model
    def get_tasks(self, params: JobGetTaskRequest) -> TasksResponse:
        """Get job tasks

        Args:
            params (JobGetTaskRequest): {
                'options': 'dict',      # Required
                'secret_data': 'dict',  # Required
                'domain_id': 'str'
            }

        Returns:
            TasksResponse: {
                'tasks': 'list'
            }

        """

        func = self.get_plugin_method("get_tasks")
        response = func(params.dict())
        return TasksResponse(**response)
