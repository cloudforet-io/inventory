import logging

from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager

_LOGGER = logging.getLogger(__name__)


RESOURCE_MAP = {
    'inventory.Server': 'ServerManager',
    'inventory.CloudService': 'CloudServiceManager'
}

class CleanupManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def list_domains(self, query):
        identity_connector = self.locator.get_connector('IdentityConnector')
        return identity_connector.list_domains(query)

    def update_collection_state(self, resource_type, hour, state, domain_id):
        """ List resource which is updated before N hours
        """
        updated_at = datetime.utcnow() - timedelta(hours=hour)
        query = {'filter': [{'k': 'updated_at', 'v': updated_at, 'o': 'lt'},
                            {'k': 'domain_id',  'v': domain_id, 'o': 'eq'},
                            {'k': 'collection_info.state',  'v': ['DISCONNECTED', 'MANUAL'], 'o': 'not_in'},
                            ]}
        #query = {}
        #query = {'filter': [{'k': 'domain_id',  'v': domain_id, 'o': 'eq'}]}

        _LOGGER.debug(f'[update_collection_state] query: {query}')
        if resource_type not in RESOURCE_MAP:
            _LOGGER.error(f'[update_collection_state] not found {resource_type}')
            return

        mgr_name = RESOURCE_MAP[resource_type]
        mgr = self.locator.get_manager(mgr_name)
        resources, total_count = mgr.update_collection_state(query, state)
        _LOGGER.debug(f'[update_collection_state] count: {resources}')


    def delete_resources_by_policy(self, resource_type, hour, method, domain_id):
        """ List resources
            state = DISCONNECTED
            updated_at < hour

        Returns: list of resources, total_count
        """
        updated_at = datetime.utcnow() - timedelta(hours=hour)
        query = {'filter': [{'k': 'updated_at', 'v': updated_at, 'o': 'lt'},
                            {'k': 'domain_id',  'v': domain_id, 'o': 'eq'},
                            {'k': 'collection_info.state',  'v': 'DISCONNECTED', 'o': 'eq'},
                            ]}
        #query = {}
        #query = {'filter': [{'k': 'domain_id',  'v': domain_id, 'o': 'eq'}]}

        _LOGGER.debug(f'[update_collection_state] query: {query}')
        if resource_type not in RESOURCE_MAP:
            _LOGGER.error(f'[delete_resources_by_policy] not found {resource_type}')
            return [], 0

        mgr_name = RESOURCE_MAP[resource_type]
        mgr = self.locator.get_manager(mgr_name)
        try:
            vos, total_count = mgr.delete_resources(query)
            return vos, total_count
        except Exception as e:
            _LOGGER.error(f'[delete_resources] {e}')
            return [], 0
