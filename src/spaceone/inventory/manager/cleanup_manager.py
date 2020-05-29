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
        mgr_name = RESOURCE_MAP[resource_type]
        mgr = self.locator.get_manager(mgr_name)
        resources, total_count = mgr.update_collection_state(query, state)
        _LOGGER.debug(f'[update_collection_state] count: {resources}')


#    def delete_resources(self, resource_type, hour, state, domain_id):
#        """ List resource which is updated before N hours
#        """
#        updated_at = datetime.utcnow() - timedelta(hours=hour)
#        query = {'filter': [{'k': 'updated_at', 'v': updated_at, 'o': 'lt'},
#                            {'k': 'domain_id',  'v': domain_id, 'o': 'eq'},
#                            {'k': 'collection_info.state',  'v': ['DISCONNECTED', 'MANUAL'], 'o': 'not_in'},
#                            ]}
#        #query = {}
#        #query = {'filter': [{'k': 'domain_id',  'v': domain_id, 'o': 'eq'}]}
# 
#        _LOGGER.debug(f'[update_collection_state] query: {query}')
#        mgr_name = RESOURCE_MAP[resource_type]
#        mgr = self.locator.get_manager(mgr_name)
#        resources, total_count = mgr.update_collection_state(query, state)
#        _LOGGER.debug(f'[update_collection_state] count: {resources}')
