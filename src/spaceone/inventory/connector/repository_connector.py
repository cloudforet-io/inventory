"""
Deprecated:
  Not used. Integrated with SpaceConnector.
"""

import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_grpc_endpoint
from spaceone.core.error import *

__all__ = ['RepositoryConnector']

_LOGGER = logging.getLogger(__name__)


class RepositoryConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)
        self._check_config()
        self._init_client()

    def _init_client(self):
        for version, uri in self.config['endpoint'].items():
            e = parse_grpc_endpoint(uri)
            self.client = pygrpc.client(endpoint=e['endpoint'], ssl_enabled=e['ssl_enabled'])

    def _check_config(self):
        if 'endpoint' not in self.config:
            raise ERROR_CONNECTOR_CONFIGURATION(backend=self.__class__.__name__)

        if len(self.config['endpoint']) > 1:
            raise ERROR_CONNECTOR_CONFIGURATION(backend=self.__class__.__name__)

    def get_plugin(self, plugin_id, domain_id):
        response = self.client.Plugin.get({
            'plugin_id': plugin_id,
            'domain_id': domain_id
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    def get_plugin_versions(self, plugin_id, domain_id):
        response = self.client.Plugin.get_versions({
            'plugin_id': plugin_id,
            'domain_id': domain_id
        }, metadata=self.transaction.get_connection_meta())

        data = self._change_message(response)
        return data['results']

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)
