import logging
from typing import Generator
from spaceone.core.service import BaseService, transaction, convert_model
from spaceone.inventory.plugin.collector.model.collector_request import CollectorInitRequest, CollectorVerifyRequest, CollectorCollectRequest
from spaceone.inventory.plugin.collector.model.collector_response import PluginResponse, ResourceResponse
from spaceone.inventory.plugin.collector.model.job_request import JobGetTaskRequest
from spaceone.inventory.plugin.collector.model.job_response import TasksResponse

_LOGGER = logging.getLogger(__name__)


class CollectorService(BaseService):

    @transaction
    @convert_model
    def init(self, params: CollectorInitRequest) -> PluginResponse:
        """ init plugin by options

        Args:
            params (CollectorInitRequest): {
                'options': 'dict',    # Required
                'domain_id': 'str'
            }

        Returns:
            PluginResponse: {
                'metadata': 'dict'
            }
        """

        func = self.get_plugin_method('init')
        response = func(params.dict())
        return PluginResponse(**response)

    @transaction
    @convert_model
    def verify(self, params: CollectorVerifyRequest) -> None:
        """ Verifying collector plugin

        Args:
            params (CollectorVerifyRequest): {
                'options': 'dict',      # Required
                'secret_data': 'dict',  # Required
                'schema': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        func = self.get_plugin_method('verify')
        func(params.dict())

    @transaction
    @convert_model
    def collect(self, params: CollectorCollectRequest) -> Generator[ResourceResponse, None, None]:
        """ Collect external data

        Args:
            params (CollectorCollectRequest): {
                'options': 'dict',      # Required
                'secret_data': 'dict',  # Required
                'schema': 'str',
                'domain_id': 'str'
            }

        Returns:
            Generator[ResourceResponse, None, None]
        """

        func = self.get_plugin_method('collect')
        response_iterator = func(params.dict())
        for response in response_iterator:
            yield ResourceResponse(**response)
