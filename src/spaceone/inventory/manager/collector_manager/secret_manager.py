import logging

from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class SecretManager(BaseManager):
    """
    Base on plugin_info from collector_vo
    This class act for Interface with real collector plugin
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.secret_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', service='secret')

    def get_secret_ids_from_provider(self, provider, domain_id):
        secrets = self.secret_connector.dispatch('Secret.list', {'provider': provider, 'domain_id': domain_id})
        _LOGGER.debug(f'[get_secret_ids_from_provider] secrets: {secrets}, by {provider}')

        result = []
        for secret in secrets.get('results', []):
            result.append(secret.get('secret_id'))

        # _LOGGER.debug(f'[get_secret_ids_from_provider] found: {secrets["total_count"]}, by {provider}')
        return result

    def get_secret_ids_from_secret_group_id(self, secret_group_id, domain_id):
        secrets = self.secret_connector.dispatch('Secret.list',
                                                 {'secret_group_id': secret_group_id, 'domain_id': domain_id})

        result = []
        for secret in secrets.get('results', []):
            result.append(secret.get('secret_id'))
        _LOGGER.debug(f'[get_secret_ids_from_secret_group_id] found: {secrets["total_count"]}, by {secret_group_id}')
        return result

    def get_secret_data(self, secret_id, domain_id):
        """
        Return: Dict type of secret
        """
        secret_data = self.secret_connector.dispatch('Secret.get_data', {'secret_id': secret_id, 'domain_id': domain_id})

        _LOGGER.debug(f'[get_secret_data] secret_data.keys: {list(secret_data)}')
        return secret_data

    def get_provider(self, secret_id, domain_id):
        """
        Return: provider in secret
        """
        secret = self.secret_connector.dispatch('Secret.get', {'secret_id': secret_id, 'domain_id': domain_id})

        _LOGGER.debug(f'[get_provider] secret: {secret}')
        return secret.get('provider', None)

    def get_secret(self, secret_id, domain_id):
        """
        Return: secret
        """
        secret = self.secret_connector.dispatch('Secret.get', {'secret_id': secret_id, 'domain_id': domain_id})

        _LOGGER.debug(f'[get_secret] secret: {secret}')
        return secret
