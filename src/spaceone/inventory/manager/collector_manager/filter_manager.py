import logging

from spaceone.core import cache
from spaceone.core.manager import BaseManager
from spaceone.inventory.error import *
from spaceone.inventory.manager.collector_manager.collecting_manager import RESOURCE_MAP

_LOGGER = logging.getLogger(__name__)


class FilterManager(BaseManager):
    """
    Transform filter for collector
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.CONSTANT_FILTER_CACHE_TIMEOUT = 86400  # 24 hours

    #####################################################
    # TODO: result of _get_collect_filter, and secret_id
    """ I want to know the founded result from _get_collect_filter must be related with secret_id
    If resource is not collected by this secret_id, I don't want to make collect call
    """

    def get_collect_filter(self, filters, plugin_info, secret_id_list=[]):
        """ Create new filters for Collect plugin's parameter
            filter_format(filters) -> new_filter

        Args:
            filters(dict): filters from Client request
            plugin_info(dict)
            secret_id_list(list)

        Returns:
            new_filter: new filters for Plugin(Collector) query
            related_secret_id_list : list of secret matched on query

        Example:
            'filter_format': [
                {
                    'change_rules': [
                        {'change_key': 'instance_id', 'resource_key': 'data.compute.instance_id'},
                        {'change_key': 'region_name', 'resource_key': 'data.compute.region'}
                    ],
                    'key': 'project_id',
                    'name': 'Project ID',
                    'resource_type': 'SERVER',
                    'search_key': 'identity.Project.project_id',
                    'type': 'str'
                },
                {
                    'change_rules': [
                        {'change_key': 'instance_id', 'resource_key': 'data.compute.instance_id'},
                        {'change_key': 'region_name', 'resource_key': 'data.compute.region'}
                    ],
                    'key': 'collection_info.service_accounts',
                    'name': 'Service Account ID',
                    'resource_type': 'SERVER',
                    'search_key': 'identity.ServiceAccount.service_account_id',
                    'type': 'str'
                },
                {
                    'change_rules': [
                        {'change_key': 'instance_id', 'resource_key': 'data.compute.instance_id'},
                        {'change_key': 'region_name', 'resource_key': 'data.compute.region'}
                    ],
                    'key': 'server_id',
                    'name': 'Server ID',
                    'resource_type': 'SERVER',
                    'search_key': 'inventory.Server.server_id',
                    'type': 'list'
                },
                {
                    'key': 'instance_id',
                    'name': 'Instance ID',
                    'resource_type': 'CUSTOM',
                    'type': 'list'
                },
                {
                    'key': 'region_name',
                    'name': 'Region',
                    'resource_type': 'CUSTOM',
                    'type': 'list'}],

            filters:
                {
                    'region_id': 'region-xxxxx',
                    'zone_id': 'zone-yyyyy',
                    'instance_id': ['i-zzzz', ...]]            # CUSTOM resource type
                    'instance_type': 'm4.xlarge'
                }

            new_filter:
                {
                    'instance_id': ['i-123', 'i-2222', ...]
                    'instance_type': 'm4.xlarge'
                }
            related_secret_id_list: ['secret-12343', 'secret-254555' ...]
            :param filters:
            :param secret_id_list:
            :param plugin_info:

        """

        metadata = plugin_info.get('metadata', None)
        #############################
        # WARNING
        # options is old spec.
        #############################
        options = plugin_info.get('options', {})
        if metadata:
            options = metadata

        filter_format = options.get('filter_format', None)
        if filter_format is None:
            _LOGGER.warning(f'[_get_collector_filter] No filter_format at plugin_info')
            return {}, secret_id_list
        if filters == {}:
            _LOGGER.debug(f'[_get_collector_filter] No filters, do nothing')
            return {}, secret_id_list

        filter_format_by_key = {}
        # filter_format_by_key['zone_id'] = {'key':'project_id', 'name':'Project ID' ...
        for item in filter_format:
            filter_format_by_key[item['key']] = item

        for filter_key, filter_value in filters.items():
            if filter_key not in filter_format_by_key:
                _LOGGER.error(f'[_get_collect_filter] unsupported filter_key: {filter_key}')
                # Strict error raise, for reducing too heavy request
                raise ERROR_UNSUPPORTED_FILTER_KEY(key=filter_key, value=filter_value)

        query_filter, custom_keys = self._prepare_query_filter(filters, filter_format_by_key)
        _LOGGER.debug(f'[_get_collect_filter] query_filter: {query_filter}, custom_keys: {custom_keys}')

        query_per_resources = self._make_query_per_resources(query_filter, filter_format_by_key)
        _LOGGER.debug(f'[_get_collect_filter] query_per_resources: {query_per_resources}')

        new_filter, filtered_secret_id_list = self._search_resources(query_per_resources,
                                                                     filter_format_by_key,
                                                                     secret_id_list)
        _LOGGER.debug(f'[_get_collect_filter] new_filter: {new_filter}')
        related_secret_id_list = _intersection(secret_id_list, filtered_secret_id_list)

        if len(custom_keys) > 0:
            new_filter = self._append_custom_keys(new_filter, filters, custom_keys)
            _LOGGER.debug(f'[_get_collect_filter] new_filter_with_custom_keys: {new_filter}')

        return new_filter, related_secret_id_list

    def cache_filter(self, collector_id, secret_id, data):
        """
        FilerManager can save cache of filter for collect plugin
        Save the region_name cache
        Args:
            data (dict): {
                'region_name': list of region name,
                'cloud_service': list of cloud service (for cloud service plugin)
                }

        Key: collector-filter:<collector_id>:<secret_id>
        Value: region_name: [list of regions]
        """
        key = f'collector-filter:{collector_id}:{secret_id}'
        _LOGGER.debug(f'[cache_filter] {key} : {data}')
        cache.set(key, data, expire=self.CONSTANT_FILTER_CACHE_TIMEOUT)

    @staticmethod
    def _get_filer_cache(collector_id, secret_id):
        key = f'collector-filter:{collector_id}:{secret_id}'
        try:
            data = cache.get(key)
            _LOGGER.debug(f'[cache_filter] {key} : {data}')
            return data
        except Exception as e:
            # May be no_cache
            return None

    def _prepare_query_filter(self, filters, filter_format_by_key):
        query_filter = {}
        """
        'region_id': [{'k': 'region_id', 'v': 'region-xxx', 'o': 'eq'}]
        'server_id': [{'k': 'server_id', 'v': 'server-yyyy', 'o': 'eq'} ....]
        ...
        """
        custom_keys = {}
        # Foreach filter, we will find matched resource list
        for filter_key, filter_value in filters.items():
            # filter_key : region_id
            filter_element = filter_format_by_key[filter_key]
            _LOGGER.debug(f'[_prepare_query_filter] filter_element: {filter_element}')
            if filter_element['resource_type'] == 'CUSTOM':
                # DO NOT save CUSTOM key at query_filter
                custom_keys[filter_key] = filter_element
                continue

            # list of new_filter[key]
            v_list = query_filter.get(filter_key, [])
            if filter_element:
                # Ask to manager, is there any matched resource
                query = self._make_query_for_manager(filter_key, filter_value, filter_element)
                if isinstance(query, list) is False:
                    _LOGGER.error("LOGIC ERROR, _make_query_for_manager does not return list value: {query}")
                else:
                    v_list.extend(query)
            query_filter[filter_key] = v_list
        return query_filter, custom_keys

    @staticmethod
    def _make_query_per_resources(query_filter, filter_format_by_key):
        # Make query per Resource
        query_per_resources = {}
        """
        'SERVER': {
            'key': 'zone_id',
            'filter': [{'k': 'region_id', 'v': 'region-xxxx', 'o': 'eq'}],
            'filter_or': [{'k': 'server_id', 'v': 'server-yyyy', 'o': 'eq'}, ...]
            }
        """
        for query_key, query in query_filter.items():
            res_type = filter_format_by_key[query_key]['resource_type']
            query_string = query_per_resources.get(res_type, {'key': query_key, 'filter': [], 'filter_or': []})
            if len(query) == 1:
                query_string['filter'].extend(query)
            elif len(query) > 1:
                query_string['filter_or'].extend(query)
            else:
                _LOGGER.debug(f'[_get_collector_filter] wrong query: {query}')
            query_per_resources[res_type] = query_string
        return query_per_resources

    def _search_resources(self, query_per_resources, filter_format_by_key, secret_id_list):
        """
        # Search Resource by Resource's Manager

        Returns: tuple of transformed query, secret_id_list
                 transformed_query {
                    'instance_id': [list of value],
                }
                 related_secret_id_list : [list of secrets]
        """
        result = {}
        # secret_id_list = []
        for res_type, query in query_per_resources.items():
            """ Example
            query: {'key': 'zone_id',
                    'filter': [
                            {'k': 'zone_id', 'v': 'zone-d710c1cb0ea7', 'o': 'eq'},
                            {'k': 'region_id', 'v': 'region-85445849c20c', 'o': 'eq'},
                            {'k': 'pool_id', 'v': 'pool-a1f35b107bb4', 'o': 'eq'}],
                    'filter_or': []}
            """
            _LOGGER.debug(f'[_search_resources] query: {query}')
            try:
                mgr = self.locator.get_manager(RESOURCE_MAP[res_type])
            except Exception as e:
                _LOGGER.error('########## NOTICE to Developer (bug) ###################################')
                _LOGGER.error(f'[_search_resources] Not found manager based on resource_type: {res_type}')
                _LOGGER.error(e)
                continue

            """
            {'change_rules': [{'change_key': 'instance_id',
                              'resource_key': 'data.compute.instance_id'},
                             {'change_key': 'region_name',
                              'resource_key': 'data.compute.region'}],
            """
            filter_element = filter_format_by_key[query['key']]
            change_rules = filter_element['change_rules']
            del query['key']

            # Ask to manager
            try:
                _LOGGER.debug(f'[_search_resources] query: {query}, key={change_rules}')
                value_list, filtered_secret_id_list = mgr.query_resources(query, change_rules)
                _LOGGER.debug(f'[_search_resources] filtered: {value_list}')
                result.update(value_list)
                secret_id_list = _intersection(secret_id_list, filtered_secret_id_list)
            except Exception as e:
                _LOGGER.error('########## NOTICE to Developer (bug) ####################################')
                _LOGGER.error(f'{res_type} Manager has bug for query_resources functions')
                _LOGGER.error(e)

        return result, secret_id_list

    @staticmethod
    def _append_custom_keys(new_filter, filters, custom_keys):
        """
        Args: {'key':'instance_id', 'name':'Instance ID', 'type':'list', 'resource_type': 'CUSTOM'}

        Return: updated new_filter
        """
        updated_filter = new_filter.copy()
        for custom_key, formats in custom_keys.items():
            _LOGGER.debug(f'[_append_custom_keys] append custom_key: {custom_key}, {formats}')
            values = filters.get(custom_key, None)
            if values is None:
                continue
            value_type = formats['type']
            _LOGGER.debug(f'[_append_custom_keys] find values: {values}, type: {value_type}')
            if value_type == 'list':
                current_value = new_filter.get(custom_key, [])
                current_value.extend(values)
                updated_filter.update({custom_key: current_value})
            elif value_type == 'str':
                current_value = new_filter.get(custom_key, None)
                if current_value:
                    _LOGGER.warning(f'[_append_custom_keys] change filter_value: {current_value} -> {values}')
                updated_filter.update({custom_key: values})
            else:
                _LOGGER.error(f'[_append_custom_keys] un-supported type: {formats}, type: {value_type}')
        _LOGGER.debug(f'[_append_custom_keys] updated_filter: {updated_filter}')
        return updated_filter

    @staticmethod
    def _make_query_for_manager(key, value, filter_element):
        """
        Args:
            key(str): key for query
            value: query value of element (str, int, bool, float, list)
            filter_element(dict): one element for filter_format

        Returns:
            query_statement (list, since there are list type)

        Example)
            value: region-xxxxx
            filter_element: {'key':'region_id', 'name':'Region', 'type':'str', 'resource_type': 'SERVER', 'change_key': ['data.compute.instance_id', 'instance_id']}
        """
        query_filter = []

        f_type = filter_element['type']
        if f_type == 'list':
            query_filter.append({
                'k': key,
                'v': value,
                'o': 'in'
            })
        elif f_type == 'str':
            query_filter.append({
                'k': key,
                'v': value,
                'o': 'eq'
            })
        else:
            _LOGGER.error(f'Unsupported filter_element, {filter_element}, supported type: list | str')
        return query_filter


def _intersection(list_a, list_b):
    """ Return intersection between list_a and list_b
    """
    if len(list_b) == 0:
        # May be user send blank list
        return list_a

    a = set(list_a)
    b = set(list_b)
    c = a.intersection(b)
    _LOGGER.debug(f'[_intersection] a: {list_a}, b: {list_b} -> {c}')
    return list(c)
