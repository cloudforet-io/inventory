import logging
from typing import Generator, Union
from spaceone.core.service import *
from spaceone.core.service.utils import *
from spaceone.inventory.plugin.collector.model.collector_request import (
    CollectorInitRequest,
    CollectorVerifyRequest,
    CollectorCollectRequest,
)
from spaceone.inventory.plugin.collector.model.collector_response import (
    PluginResponse,
    ResourceResponse,
)

_LOGGER = logging.getLogger(__name__)


class CollectorService(BaseService):
    resource = "Collector"

    @transaction
    @convert_model
    def init(self, params: CollectorInitRequest) -> Union[dict, PluginResponse]:
        """init plugin by options

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

        func = self.get_plugin_method("init")
        response = func(params.dict())
        return PluginResponse(**response)

    @transaction
    @convert_model
    def verify(self, params: CollectorVerifyRequest) -> None:
        """Verifying collector plugin

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

        func = self.get_plugin_method("verify")
        func(params.dict())

    @transaction
    @convert_model
    def collect(
        self, params: CollectorCollectRequest
    ) -> Union[Generator[ResourceResponse, None, None], dict]:
        """Collect external data

        Args:
            params (CollectorCollectRequest): {
                'options': 'dict',      # Required
                'secret_data': 'dict',  # Required
                'schema': 'str',
                'task_options': 'dict',
                'domain_id': 'str'
            }

        Returns:
            Generator[ResourceResponse, None, None]
        """
        func = self.get_plugin_method("collect")
        response_iterator = func(params.dict())
        for response in response_iterator:
            yield ResourceResponse(**response)
