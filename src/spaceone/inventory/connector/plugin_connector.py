"""
Deprecated:
  Not used. Integrated with SpaceConnector.
"""

import logging

from spaceone.core import pygrpc
from spaceone.core.connector import BaseConnector
from spaceone.core.error import ERROR_WRONG_CONFIGURATION
from spaceone.core.utils import parse_grpc_endpoint

_LOGGER = logging.getLogger(__name__)


class PluginConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)
        self._check_config()
        self._init_client()

    def _init_client(self):
        for version, uri in self.config['endpoint'].items():
            e = parse_grpc_endpoint(uri)
            self.client = pygrpc.client(endpoint=e['endpoint'], ssl_enabled=e['ssl_enabled'])

    def _check_config(self):
        _LOGGER.debug(f'[_check_config] config: {self.config}')
        if 'endpoint' not in self.config:
            raise ERROR_WRONG_CONFIGURATION(key='endpoint')
        if len(self.config['endpoint']) > 1:
            raise ERROR_WRONG_CONFIGURATION(key='too many endpoint')

    def get_plugin_endpoint(self, plugin_id, version, domain_id, upgrade_mode='AUTO'):
        """
        Returns: endpoint (message)
        """
        params = {
            'plugin_id': plugin_id,
            'version': version,
            'upgrade_mode': upgrade_mode,
            'domain_id': domain_id
        }

        meta: list = self.transaction.get_connection_meta()
        #meta.append(('token', self.system_key), )
        #_LOGGER.debug('[get_plugin_endpoint] meta:%s' % meta)
        response = self.client.Plugin.get_plugin_endpoint(
            params,
            metadata=meta
        )
        return response
