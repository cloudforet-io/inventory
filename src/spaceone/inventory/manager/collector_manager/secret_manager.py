import logging

from google.protobuf.json_format import MessageToDict
from spaceone.core.manager import BaseManager

_LOGGER = logging.getLogger(__name__)


class SecretManager(BaseManager):
    """
    Base on plugin_info from collector_vo
    This class act for Interface with real collector plugin
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_secret_ids_from_provider(self, provider, domain_id):
        secret_connector = self.locator.get_connector('SecretConnector')
        secrets = secret_connector.list_secrets_by_provider(provider, domain_id)
        result = []
        for secret in secrets.results:
            result.append(secret.secret_id)
        _LOGGER.debug(f'[get_secret_ids_from_provider] found: {secrets.total_count}, by {provider}')
        return result

    def get_secret_ids_from_secret_group_id(self, secret_group_id, domain_id):
        secret_connector = self.locator.get_connector('SecretConnector')
        secrets = secret_connector.list_secrets_by_secret_group_id(secret_group_id, domain_id)
        result = []
        for secret in secrets.results:
            result.append(secret.secret_id)
        _LOGGER.debug(f'[get_secret_ids_from_secret_group_id] found: {secrets.total_count}, by {secret_group_id}')
        return result

    def get_secret_data(self, secret_id, domain_id):
        """
        Return: Dict type of secret
        """
        secret_connector = self.locator.get_connector('SecretConnector')
        secret_data = secret_connector.get_secret_data(secret_id, domain_id)
        secret_data_dict = MessageToDict(secret_data, preserving_proto_field_name=True)
        _LOGGER.debug(f'[get_secret_data] secret_data.keys: {list(secret_data_dict)}')
        return secret_data

    def get_provider(self, secret_id, domain_id):
        """
        Return: provider in secret
        """
        secret_connector = self.locator.get_connector('SecretConnector')
        secret = secret_connector.get_secret(secret_id, domain_id)
        secret_dict = MessageToDict(secret, preserving_proto_field_name=True)
        _LOGGER.debug(f'[get_provider] secret: {secret}')
        return secret_dict.get('provider', None)

    def get_secret(self, secret_id, domain_id):
        """
        Return: secret
        """
        secret_connector = self.locator.get_connector('SecretConnector')
        secret = secret_connector.get_secret(secret_id, domain_id)
        secret_dict = MessageToDict(secret, preserving_proto_field_name=True)
        _LOGGER.debug(f'[get_secret] secret: {secret_dict}')
        return secret_dict
