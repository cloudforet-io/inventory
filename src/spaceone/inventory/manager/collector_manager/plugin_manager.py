import logging

from spaceone.core.manager import BaseManager

from spaceone.inventory.error import *
from spaceone.inventory.manager.collector_manager.secret_manager import SecretManager

__ALL__ = ['PluginManager']

_LOGGER = logging.getLogger(__name__)


"""
Base on plugin_info from collector_vo
This class act for general interface with real collector plugin
"""
class PluginManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def init(self, params):
        """ Init plugin with params.plugin_info

        Returns: plugin_info(metadata)
        """
        plugin_info = params.get('plugin_info', {})
        domain_id = params['domain_id']
        return self._init_by_plugin_info(plugin_info, domain_id)

    def verify(self, params):
        """ Verify plugin with params.plugin_info
        After verify, plugin_info.options will be updated

        Returns: verified_params
        """
        plugin_info = params.get('plugin_info', {})
        domain_id = params['domain_id']
        return self.verify_by_plugin_info(plugin_info, domain_id)

    def verify_by_plugin_info(self, plugin_info, domain_id, secret_id=None):
        self._check_plugin_info(plugin_info)
        plugin_id = plugin_info['plugin_id']
        version   = plugin_info['version']
        labels    = plugin_info.get('labels', {})
        options   = plugin_info.get('options', {})

        secret_id_list = self.get_secrets_from_plugin_info(plugin_info, domain_id, secret_id)

        endpoint = self.get_endpoint(plugin_id, version, domain_id, labels)
        _LOGGER.debug(f'[verify] endpoint: {endpoint} of plugin: {plugin_id}, {version}, {len(secret_id_list)}')
        verified = False
        for secret_id in secret_id_list:
            try:
                secret_data = self._get_secret_data(secret_id, domain_id)
                _LOGGER.debug(f'[verify] secret_data.keys: {list(secret_data)}')
                verified_options = self.verify_plugin(endpoint, options, secret_data)
                verified = True
            except Exception as e:
                _LOGGER.debug(f'[verify] {e}')
                _LOGGER.warn(f'[verify] fail to verify with secret: {secret_id}')
        if verified and verified_options != None:
            return verified_options
        raise ERROR_VERIFY_PLUGIN_FAILURE(params=plugin_info)

    def get_secrets_from_plugin_info(self, plugin_info, domain_id, secret_id=None):
        self._check_plugin_info(plugin_info)

        secret_id_list = self._get_secret_id_list(plugin_info, domain_id)
        if secret_id:
            if is_member(secret_id, secret_id_list):
                secret_id_list = [secret_id]
            else:
                _LOGGER.error(f'[verify_by_plugin_info] {secret_id} is not a member of {secret_id_list}')
                raise ERROR_VERIFY_PLUGIN_FAILURE(params=secret_id)

        _LOGGER.debug(f'[verify] secret_id_list: {secret_id_list}')
        return secret_id_list

    def get_endpoint(self, plugin_id, version, domain_id, labels={}):
        """ Get plugin endpoint
        """
        plugin_connector = self.locator.get_connector('PluginConnector')
        return plugin_connector.get_plugin_endpoint(plugin_id, version, domain_id, labels)

    def init_plugin(self, endpoint, options):
        """ Init plugin
        """
        connector = self.locator.get_connector('CollectorPluginConnector')
        connector.initialize(endpoint)
        return connector.init(options)

    def verify_plugin(self, endpoint, options, secret_data):
        """ Verify plugin
        """
        connector = self.locator.get_connector('CollectorPluginConnector')
        connector.initialize(endpoint)
        return connector.verify(options, secret_data)

    def _check_plugin_info(self, plugin_info):
        """ Plugin Info has
            - plugin_id (mendatory)
            - version   (mendatory)
            - labels    (optional)
            - options   (optional)
            - secret_group_id   (optional)
            - secret_id         (optional)
            - provider          (optional)

        Returns: True
        Raise: ERROR_PLUGIN_PARAMETER
        """
        mendatory = ['plugin_id', 'version']
        for item in mendatory:
            if item not in plugin_info:
                raise ERROR_NO_PLUGIN_PARAMETER(param=item)

        return True

    def _get_secret_id_list(self, plugin_info, domain_id):
        """
        Return: list of secret ID
        """
        secret_group_id = plugin_info.get('secret_group_id', None)
        secret_id = plugin_info.get('secret_id', None)
        provider = plugin_info.get('provider', None)

        _LOGGER.debug(f'[_get_secret_id_list] {secret_id}, {secret_group_id}, {provider}')
        if provider and (secret_group_id or secret_id):
            _LOGGER.warning(f'[_get_secret_id_list] both provider and (secret_group_id or secret_id) \
                           exist at {plugin_info}')
            _LOGGER.warning(f'[_get_secret_id_list] use minimum set: {secret_group_id} or {secret_id}')
            provider = None

        secret_mgr = self.locator.get_manager('SecretManager')
        result_list = []
        if provider:
            result_list.extend(secret_mgr.get_secret_ids_from_provider(provider, domain_id))
        if secret_group_id:
            result_list.extend(secret_mgr.get_secret_ids_from_secret_group_id(secret_group_id))
        if secret_id:
            result_list.append(secret_id)
        return result_list

    def _get_secret_data(self, secret_id, domain_id):
        """
        Return: secret_data (as dict format)
        """
        secret_mgr = self.locator.get_manager('SecretManager')
        secret_data = secret_mgr.get_secret_data(secret_id, domain_id)
        return secret_data.data

    def _init_by_plugin_info(self, plugin_info, domain_id):
        self._check_plugin_info(plugin_info)
        plugin_id = plugin_info['plugin_id']
        version   = plugin_info['version']
        options   = plugin_info.get('options', {})
        labels    = plugin_info.get('labels', {})

        endpoint = self.get_endpoint(plugin_id, version, domain_id, labels)
        _LOGGER.debug(f'[verify] endpoint: {endpoint} of plugin: {plugin_id}, {version}')
        plugin_meta = self.init_plugin(endpoint, options)
        _LOGGER.debug(f'[_init_by_plugin_info] metadata: {plugin_meta}')
        return plugin_meta


def is_member(item, seq):
    return sum(map(lambda x: x == item, seq)) > 0
