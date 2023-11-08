import logging
import copy
from datetime import datetime

from spaceone.core.manager import BaseManager
from spaceone.core import utils
from spaceone.core.error import *
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.lib.resource_manager import ResourceManager
from spaceone.inventory.manager.collection_state_manager import CollectionStateManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.manager.reference_manager import ReferenceManager

_LOGGER = logging.getLogger(__name__)

MERGE_KEYS = [
    'name',
    'ip_addresses',
    'account',
    'instance_type',
    'instance_size',
    'reference'
    'region_code',
    'ref_region',
    'project_id',
    'data'
]


class CloudServiceManager(BaseManager, ResourceManager):
    resource_keys = ['cloud_service_id']
    query_method = 'list_cloud_services'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_model: CloudService = self.locator.get_model('CloudService')

    def create_cloud_service(self, params):
        def _rollback(cloud_svc_vo: CloudService):
            _LOGGER.info(
                f'[ROLLBACK] Delete Cloud Service : {cloud_svc_vo.provider} ({cloud_svc_vo.cloud_service_type})')
            cloud_svc_vo.terminate()

        cloud_svc_vo: CloudService = self.cloud_svc_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_vo)

        return cloud_svc_vo

    def update_cloud_service(self, params):
        return self.update_cloud_service_by_vo(params,
                                               self.get_cloud_service(params['cloud_service_id'], params['domain_id']))

    def update_cloud_service_by_vo(self, params, cloud_svc_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("cloud_service_id")}')
            cloud_svc_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cloud_svc_vo.to_dict())
        cloud_svc_vo: CloudService = cloud_svc_vo.update(params)

        return cloud_svc_vo

    def delete_cloud_service(self, cloud_service_id, domain_id):
        cloud_svc_vo = self.get_cloud_service(cloud_service_id, domain_id)
        cloud_svc_vo.delete()

    @staticmethod
    def delete_cloud_service_by_vo(cloud_svc_vo: CloudService):
        cloud_svc_vo.delete()

    def terminate_cloud_service(self, cloud_service_id, domain_id):
        cloud_svc_vo: CloudService = self.get_cloud_service(cloud_service_id, domain_id)
        cloud_svc_vo.terminate()

    def get_cloud_service(self, cloud_service_id, domain_id, user_projects=None, only=None):
        kwargs = {
            'cloud_service_id': cloud_service_id,
            'domain_id': domain_id
        }

        if user_projects:
            kwargs['project_id'] = user_projects

        if only:
            kwargs['only'] = only

        return self.cloud_svc_model.get(**kwargs)

    def list_cloud_services(self, query, target=None, change_filter=False, domain_id=None):
        if change_filter:
            query = self._change_project_group_filter(query, domain_id)
            query = self._change_filter_tags(query)
            query = self._change_only_tags(query)
            query = self._change_sort_tags(query)

        # Append Query for DELETED filter (Temporary Logic)
        query = self._append_state_query(query)
        return self.cloud_svc_model.query(**query, target=target)

    def get_export_query_results(self, options, domain_id, user_projects=None):
        ref_mgr: ReferenceManager = self.locator.get_manager(ReferenceManager)

        for export_option in options:
            self._check_export_option(export_option)

            if export_option['query_type'] == 'SEARCH':
                export_option['search_query'] = self._change_export_query(
                    'SEARCH', export_option['search_query'], domain_id, user_projects)
                export_option['results'] = self._get_search_query_results(export_option['search_query'], domain_id,
                                                                          ref_mgr)
            else:
                export_option['analyze_query'] = self._change_export_query(
                    'ANALYZE', export_option['analyze_query'], domain_id, user_projects)
                export_option['results'] = self._get_analyze_query_results(export_option['analyze_query'], domain_id)

        return options

    def _get_search_query_results(self, query, domain_id, ref_mgr: ReferenceManager):
        cloud_service_vos, total_count = self.list_cloud_services(query, change_filter=True, domain_id=domain_id)
        results = []

        fields = query.get('fields')
        if fields is None:
            raise ERROR_REQUIRED_PARAMETER(key='options[].search_query.fields')

        for cloud_service_vo in cloud_service_vos:
            cloud_service_data = cloud_service_vo.to_dict()

            result = {}
            for field in fields:
                if isinstance(field, dict):
                    key = field['key']
                    name = field.get('name') or key
                    reference = field.get('reference', {})

                    value = utils.get_dict_value(cloud_service_data, key)

                    if resource_type := reference.get('resource_type'):
                        if isinstance(value, list):
                            value = [ref_mgr.get_reference_name(resource_type, v, domain_id) for v in value]
                        else:
                            value = ref_mgr.get_reference_name(resource_type, value, domain_id)

                else:
                    key = field
                    name = field
                    value = utils.get_dict_value(cloud_service_data, key)

                if key in ['created_at', 'updated_at', 'deleted_at']:
                    name = f'{name} (UTC)'

                if isinstance(value, list):
                    value_str = [str(v) for v in value]
                    value = '\n'.join(value_str)

                result[name] = value

            results.append(result)

        return results

    def _get_analyze_query_results(self, query, domain_id):
        response = self.analyze_cloud_services(query, change_filter=True, domain_id=domain_id)
        return response.get('results', [])

    def analyze_cloud_services(self, query, change_filter=False, domain_id=None):
        if change_filter:
            query = self._change_project_group_filter(query, domain_id)
            query = self._change_filter_tags(query)

        # Append Query for DELETED filter (Temporary Logic)
        query = self._append_state_query(query)
        return self.cloud_svc_model.analyze(**query)

    def stat_cloud_services(self, query, change_filter=False, domain_id=None):
        if change_filter:
            query = self._change_project_group_filter(query, domain_id)
            query = self._change_filter_tags(query)
            query = self._change_distinct_tags(query)

        # Append Query for DELETED filter (Temporary Logic)
        query = self._append_state_query(query)
        return self.cloud_svc_model.stat(**query)

    @staticmethod
    def merge_data(new_data, old_data):
        for key in MERGE_KEYS:
            if key in new_data:
                new_value = new_data[key]
                old_value = old_data.get(key)
                if key in ['data', 'tags']:
                    is_changed = False
                    for sub_key, sub_value in new_value.items():
                        if sub_value != old_value.get(sub_key):
                            is_changed = True
                            break

                    if is_changed:
                        merged_value = copy.deepcopy(old_value)
                        merged_value.update(new_value)
                        new_data[key] = merged_value
                    else:
                        del new_data[key]
                else:
                    if new_value == old_value:
                        del new_data[key]

        return new_data

    def find_resources(self, query):
        query['only'] = ['cloud_service_id']

        resources = []
        cloud_svc_vos, total_count = self.list_cloud_services(query, target='SECONDARY_PREFERRED')

        for cloud_svc_vo in cloud_svc_vos:
            resources.append({
                'cloud_service_id': cloud_svc_vo.cloud_service_id
            })

        return resources, total_count

    def delete_resources(self, query):
        query['only'] = self.resource_keys

        vos, total_count = self.list_cloud_services(query)

        resource_ids = []
        for vo in vos:
            resource_ids.append(vo.cloud_service_id)

        vos.update({
            'state': 'DELETED',
            'deleted_at': datetime.utcnow()
        })

        state_mgr: CollectionStateManager = self.locator.get_manager('CollectionStateManager')
        state_mgr.delete_collection_state_by_resource_ids(resource_ids)

        return total_count

    @staticmethod
    def _append_state_query(query):
        state_default_filter = {
            'key': 'state',
            'value': 'ACTIVE',
            'operator': 'eq'
        }

        show_deleted_resource = False
        for condition in query.get('filter', []):
            key = condition.get('k', condition.get('key'))
            value = condition.get('v', condition.get('value'))
            operator = condition.get('o', condition.get('operator'))

            if key == 'state':
                if operator == 'eq' and value == 'DELETED':
                    show_deleted_resource = True
                elif operator in ['in', 'contain_in'] and 'DELETED' in value:
                    show_deleted_resource = True

        if not show_deleted_resource:
            query['filter'] = query.get('filter', [])
            query['filter'].append(state_default_filter)

        return query

    def _change_project_group_filter(self, query, domain_id):
        change_filter = []

        project_group_query = {
            'filter': [],
            'only': ['project_group_id']
        }

        for condition in query.get('filter', []):
            key = condition.get('key', condition.get('k'))
            value = condition.get('value', condition.get('v'))
            operator = condition.get('operator', condition.get('o'))

            if not all([key, operator]):
                raise ERROR_DB_QUERY(reason='filter condition should have key, value and operator.')

            if key == 'project_group_id':
                project_group_query['filter'].append(condition)
            else:
                change_filter.append(condition)

        if len(project_group_query['filter']) > 0:
            identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
            response = identity_mgr.list_project_groups(project_group_query, domain_id)
            project_group_ids = []
            project_ids = []
            for project_group_info in response.get('results', []):
                project_group_ids.append(project_group_info['project_group_id'])

            for project_group_id in project_group_ids:
                response = identity_mgr.list_projects_in_project_group(project_group_id, domain_id, True,
                                                                       {'only': ['project_id']})
                for project_info in response.get('results', []):
                    if project_info['project_id'] not in project_ids:
                        project_ids.append(project_info['project_id'])

            change_filter.append({'k': 'project_id', 'v': project_ids, 'o': 'in'})

        query['filter'] = change_filter
        return query

    def _change_filter_tags(self, query):
        change_filter = []

        for condition in query.get('filter', []):
            key = condition.get('k', condition.get('key'))
            value = condition.get('v', condition.get('value'))
            operator = condition.get('o', condition.get('operator'))

            if key.startswith('tags.'):
                hashed_key = self._get_hashed_key(key)

                change_filter.append({
                    'key': hashed_key,
                    'value': value,
                    'operator': operator
                })

            else:
                change_filter.append(condition)
        query['filter'] = change_filter
        return query

    def _change_only_tags(self, query):
        change_only_tags = []
        if 'only' in query:
            for key in query.get('only', []):
                if key.startswith('tags.'):
                    hashed_key = self._get_hashed_key(key, only=True)
                    change_only_tags.append(hashed_key)
                else:
                    change_only_tags.append(key)
            query['only'] = change_only_tags

        return query

    def _change_distinct_tags(self, query):
        if 'distinct' in query:
            distinct_key = query['distinct']
            if distinct_key.startswith('tags.'):
                hashed_key = self._get_hashed_key(distinct_key)
                query['distinct'] = hashed_key

        return query

    def _change_sort_tags(self, query):
        if 'sort' in query:
            if 'keys' in query['sort']:
                change_filter = []
                sort_keys = query['sort'].get('keys', [])
                for condition in sort_keys:
                    sort_key = condition.get('key', '')
                    desc = condition.get('desc', False)

                    if sort_key.startswith('tags.'):
                        hashed_key = self._get_hashed_key(sort_key)

                        change_filter.append({
                            'key': hashed_key,
                            'desc': desc
                        })

                    else:
                        change_filter.append({
                            'key': sort_key,
                            'desc': desc
                        })
                query['sort']['keys'] = change_filter

            elif 'key' in query['sort']:
                change_filter = {}
                sort_key = query['sort']['key']
                desc = query['sort'].get('desc', False)

                if sort_key.startswith('tags.'):
                    hashed_key = self._get_hashed_key(sort_key)

                    change_filter.update({
                        'key': hashed_key,
                        'desc': desc
                    })

                else:
                    change_filter.update({
                        'key': sort_key,
                        'desc': desc
                    })
                query['sort'] = change_filter
        return query

    @staticmethod
    def _get_hashed_key(key, only=False):
        if key.count('.') < 2:
            return key

        prefix, provider, key = key.split('.', 2)
        hash_key = utils.string_to_hash(key)
        if only:
            return f'{prefix}.{provider}.{hash_key}'
        else:
            return f'{prefix}.{provider}.{hash_key}.value'

    @staticmethod
    def _check_export_option(export_option):
        if 'name' not in export_option:
            raise ERROR_REQUIRED_PARAMETER(key='options[].name')

        query_type = export_option.get('query_type')

        if query_type == 'SEARCH':
            if 'search_query' not in export_option:
                raise ERROR_REQUIRED_PARAMETER(key='options[].search_query')
        elif query_type == 'ANALYZE':
            if 'analyze_query' not in export_option:
                raise ERROR_REQUIRED_PARAMETER(key='options[].analyze_query')
        else:
            raise ERROR_REQUIRED_PARAMETER(key='options[].query_type')

    @staticmethod
    def _change_export_query(query_type, query, domain_id, user_projects=None):
        query['filter'] = query.get('filter', [])
        query['filter_or'] = query.get('filter_or', [])
        keyword = query.get('keyword')

        query['filter'].append({'k': 'domain_id', 'v': domain_id, 'o': 'eq'})
        if user_projects:
            query['filter'].append({'k': 'user_projects', 'v': user_projects, 'o': 'in'})

        if keyword:
            keyword = keyword.strip()
            if len(keyword) > 0:
                for key in ['cloud_service_id', 'name', 'ip_addresses', 'cloud_service_group',
                            'cloud_service_type', 'reference.resource_id']:
                    query['filter_or'].append({
                        'k': key,
                        'v': list(filter(None, keyword.split(' '))),
                        'o': 'contain_in'
                    })

            del query['keyword']

        if query_type == 'SEARCH':
            query['only'] = []
            fields = query.get('fields', [])
            for field in fields:
                if isinstance(field, dict):
                    if key := field.get('key'):
                        query['only'].append(key)
                    else:
                        raise ERROR_REQUIRED_PARAMETER(key='options[].search_query.fields.key')
                elif isinstance(field, str):
                    query['only'].append(field)
                else:
                    raise ERROR_INVALID_PARAMETER_TYPE(key='options[].search_query.fields', type='str or dict')

            # Code for Query Compatibility
            sort = query.get('sort', [])
            if len(sort) > 0:
                query['sort'] = {
                    'keys': sort
                }

        return query
