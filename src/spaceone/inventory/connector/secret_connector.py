"""
Deprecated:
  Not used. Integrated with SpaceConnector.
"""

import logging

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_grpc_endpoint
from spaceone.core.error import *

__all__ = ['SecretConnector']
_LOGGER = logging.getLogger(__name__)


class SecretConnector(BaseConnector):
    def __init__(self, transaction, config):
        super().__init__(transaction, config)

        if 'endpoint' not in self.config:
            raise ERROR_WRONG_CONFIGURATION(key='endpoint')

        if len(self.config['endpoint']) > 1:
            raise ERROR_WRONG_CONFIGURATION(key='too many endpoint')

        for version, uri in self.config['endpoint'].items():
            e = parse_grpc_endpoint(uri)
            self.client = pygrpc.client(endpoint=e['endpoint'], ssl_enabled=e['ssl_enabled'])

    def get_secret(self, secret_id, domain_id):
        return self.client.Secret.get({'secret_id': secret_id, 'domain_id': domain_id},
                                      metadata=self.transaction.get_connection_meta())

    def list_secrets_by_secret_group_id(self, secret_group_id, domain_id):
        return self.client.Secret.list({'secret_group_id': secret_group_id, 'domain_id': domain_id},
                                       metadata=self.transaction.get_connection_meta())

    def list_secrets_by_provider(self, provider, domain_id):
        return self.client.Secret.list({'provider': provider, 'domain_id': domain_id},
                                       metadata=self.transaction.get_connection_meta())

    def get_secret_data(self, secret_id, domain_id):
        return self.client.Secret.get_data({'secret_id': secret_id, 'domain_id': domain_id},
                                           metadata=self.transaction.get_connection_meta())
