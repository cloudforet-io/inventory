# -*- coding: utf-8 -*-
import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_endpoint
from spaceone.core.error import *


__all__ = ['ConfigConnector']
_LOGGER = logging.getLogger(__name__)


class ConfigConnector(BaseConnector):
    def __init__(self, transaction, config):
        super().__init__(transaction, config)

        if 'endpoint' not in self.config:
            raise ERROR_WRONG_CONFIGURATION(key='endpoint')

        if len(self.config['endpoint']) > 1:
            raise ERROR_WRONG_CONFIGURATION(key='too many endpoint')

        for (k, v) in self.config['endpoint'].items():
            e = parse_endpoint(v)
            self.client = pygrpc.client(endpoint=f'{e.get("hostname")}:{e.get("port")}', version=k)

    def get_domain_config(self, name, domain_id):
        return MessageToDict(self.client.DomainConfig.get({'name': name, 'domain_id': domain_id},
                                                  metadata=self.transaction.get_connection_meta()),
                             preserving_proto_field_name=True)

