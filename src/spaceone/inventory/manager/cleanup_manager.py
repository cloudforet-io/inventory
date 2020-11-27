import logging
import re

from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager

_LOGGER = logging.getLogger(__name__)


RESOURCE_MAP = {
    'inventory.Server': 'ServerManager',
    'inventory.CloudService': 'CloudServiceManager'
}

OP_MAP = {
    '='  : 'eq',
    '>=' : 'gte',
    '<=' : 'lte',
    '>'  : 'gt',
    '<'  : 'lt',
    '!=' : 'not'
}

class CleanupManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def list_domains(self, query):
        identity_connector = self.locator.get_connector('IdentityConnector')
        return identity_connector.list_domains(query)

    def update_collection_state(self, resource_type, hour, state, domain_id):
        """ List resource which is updated before N hours

        Args:
            resource_type: str

        Example of resource type
         - inventory.Server
         - inventory.Server?data.aws.lifecycle=spot
         - inventory.CloudService?provider=aws&cloud_service_group=DynamoDB
        """
        updated_at = datetime.utcnow() - timedelta(hours=hour)
       #query = {}
        #query = {'filter': [{'k': 'domain_id',  'v': domain_id, 'o': 'eq'}]}
        resource_type_name, my_filter_list = self._parse_resource_type(resource_type)

        print(my_filter_list)
        my_filter_list.extend(
                            [{'k': 'updated_at', 'v': updated_at, 'o': 'lt'},
                            {'k': 'domain_id',  'v': domain_id, 'o': 'eq'},
                            {'k': 'collection_info.state',  'v': ['DISCONNECTED', 'MANUAL'], 'o': 'not_in'},
                            ]
                    )
        query = {'filter': my_filter_list}
        print(my_filter_list)
        print(query)
        _LOGGER.debug(f'[update_collection_state] query: {query}')
        if resource_type_name not in RESOURCE_MAP:
            _LOGGER.error(f'[update_collection_state] not found {resource_type_name}')
            return

        mgr_name = RESOURCE_MAP[resource_type_name]
        mgr = self.locator.get_manager(mgr_name)
        resources, total_count = mgr.update_collection_state(query, state)
        _LOGGER.debug(f'[update_collection_state] {resource_type}, {total_count}, {state} in {domain_id}')


    def delete_resources_by_policy(self, resource_type, hour, state, domain_id):
        """ List resources
            state = DISCONNECTED
            updated_at < hour

        Returns: list of resources, total_count
        """
        updated_at = datetime.utcnow() - timedelta(hours=hour)

        resource_type_name, my_filter_list = self._parse_resource_type(resource_type)

        my_filter_list.extend(
                        [{'k': 'updated_at', 'v': updated_at, 'o': 'lt'},
                        {'k': 'domain_id',  'v': domain_id, 'o': 'eq'},
                        {'k': 'collection_info.state',  'v': 'DISCONNECTED', 'o': 'eq'},
                        ]
                )
        query = {'filter': my_filter_list}

        #query = {}
        #query = {'filter': [{'k': 'domain_id',  'v': domain_id, 'o': 'eq'}]}

        _LOGGER.debug(f'[update_collection_state] query: {query}')
        if resource_type_name not in RESOURCE_MAP:
            _LOGGER.error(f'[delete_resources_by_policy] not found {resource_type_name}')
            return [], 0

        mgr_name = RESOURCE_MAP[resource_type_name]
        mgr = self.locator.get_manager(mgr_name)
        try:
            vos, total_count = mgr.delete_resources(query, state)
            _LOGGER.debug(f'[delete_resources_by_policy] {total_count}, {state} in {domain_id}')
            return vos, total_count
        except Exception as e:
            _LOGGER.error(f'[delete_resources] {e}')
            return [], 0

    def _parse_resource_type(self, resource_type):
        """
        Example of resource type
         - inventory.Server
         - inventory.Server?data.aws.lifecycle=spot
         - inventory.Server?data.auto_scaling_group!=
         - inventory.CloudService?provider=aws&cloud_service_group=DynamoDB
        """
        items = resource_type.split('?')
        resource_type_name = items[0]
        filter_list = []
        if len(items) == 2:
            kv = items[1]
            kv_items = kv.split('&')
            for kv_item in kv_items:
                item = re.compile(r'(?P<k>[\w.]+)(?P<o>=|!=|<=|<|>=|>)(?P<v>[\w.]*)')
                m = item.match(kv_item)
                kvo = m.groupdict()
                if kvo['v'] == '':
                    value = None
                else:
                    value = kvo['v']
                filter_list.append({'k': kvo['k'], 'o': OP_MAP[kvo["o"]], 'v': value})
                #item = kv_item.split('=')
                #if len(item) == 2:
                #    filter_list.append((item[0], item[1]))
        elif len(items) > 2:
            # Raise wrong format
            _LOGGER.error(f'[_parse_resource_type] wrong format: {resource_type}')

        _LOGGER.debug(f'[_parse_resource_type] {resource_type} -> {resource_type_name}, {filter_list}')
        return resource_type_name, filter_list
