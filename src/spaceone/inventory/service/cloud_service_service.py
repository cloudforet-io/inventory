import logging
import copy
from datetime import datetime
from typing import List

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_model import CloudService, CollectionInfo
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.manager.resource_group_manager import ResourceGroupManager
from spaceone.inventory.manager.change_history_manager import ChangeHistoryManager
from spaceone.inventory.manager.collection_state_manager import CollectionStateManager
from spaceone.inventory.manager.note_manager import NoteManager
from spaceone.inventory.manager.collector_rule_manager import CollectorRuleManager
from spaceone.inventory.manager.export_manager import ExportManager
from spaceone.inventory.error import *

_KEYWORD_FILTER = ['cloud_service_id', 'name', 'ip_addresses', 'cloud_service_group', 'cloud_service_type',
                   'reference.resource_id']

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')
        self.region_mgr: RegionManager = self.locator.get_manager('RegionManager')
        self.identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        self.collector_rule_mgr: CollectorRuleManager = self.locator.get_manager('CollectorRuleManager')
        self.collector_id = self.transaction.get_meta('collector_id')
        self.job_id = self.transaction.get_meta('job_id')
        self.plugin_id = self.transaction.get_meta('plugin_id')
        self.service_account_id = self.transaction.get_meta('secret.service_account_id')

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'authorization.require_project_id': True
    })
    def create(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_type': 'str',
                    'cloud_service_group': 'str',
                    'provider': 'str',
                    'name': 'str',
                    'account': 'str',
                    'instance_type': 'str',
                    'instance_size': 'float',
                    'ip_addresses': 'list',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'list or dict',
                    'region_code': 'str',
                    'project_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_vo (object)

        """
        return self.create_resource(params)

    @check_required(['cloud_service_type', 'cloud_service_group', 'provider', 'data', 'domain_id'])
    def create_resource(self, params):
        ch_mgr: ChangeHistoryManager = self.locator.get_manager('ChangeHistoryManager')

        domain_id = params['domain_id']
        secret_project_id = self.transaction.get_meta('secret.project_id')
        provider = params['provider']

        if 'instance_size' in params:
            if not isinstance(params['instance_size'], float):
                raise ERROR_INVALID_PARAMETER_TYPE(key='instance_size', type='float')

        if 'tags' in params:
            params['tags'] = self._convert_tags_to_dict(params['tags'])

        # Change data through Collector Rule
        if self._is_created_by_collector():
            params = self.collector_rule_mgr.change_cloud_service_data(self.collector_id, domain_id, params)

        if 'tags' in params:
            params['tags'], params['tag_keys'] = self._convert_tags_to_hash(params['tags'], provider)

        if 'project_id' in params:
            self.identity_mgr.get_project(params['project_id'], domain_id)
        elif secret_project_id:
            params['project_id'] = secret_project_id

        params['ref_cloud_service_type'] = self._make_cloud_service_type_key(params)

        if 'region_code' in params:
            params['ref_region'] = self._make_region_key(params, params['provider'])

        if 'metadata' in params:
            params['metadata'] = self._convert_metadata(params['metadata'], provider)

        params['collection_info'] = self._get_collection_info(provider)

        cloud_svc_vo = self.cloud_svc_mgr.create_cloud_service(params)

        # Create New History
        ch_mgr.add_new_history(cloud_svc_vo, params)

        # Create Collection State
        state_mgr: CollectionStateManager = self.locator.get_manager('CollectionStateManager')
        state_mgr.create_collection_state(cloud_svc_vo.cloud_service_id, 'inventory.CloudService', domain_id)

        return cloud_svc_vo

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    def update(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'name': 'str',
                    'account': 'str',
                    'instance_type': 'str',
                    'instance_size': 'float',
                    'ip_addresses': 'list',
                    'data': 'dict',
                    'metadata': 'dict',
                    'reference': 'dict',
                    'tags': 'list or dict',
                    'region_code': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'release_project': 'bool',
                    'release_region': 'bool'
                }

        Returns:
            cloud_service_vo (object)
        """
        return self.update_resource(params)

    @check_required(['cloud_service_id', 'domain_id'])
    def update_resource(self, params):
        ch_mgr: ChangeHistoryManager = self.locator.get_manager('ChangeHistoryManager')

        secret_project_id = self.transaction.get_meta('secret.project_id')

        cloud_service_id = params['cloud_service_id']
        domain_id = params['domain_id']
        release_region = params.get('release_region', False)
        release_project = params.get('release_project', False)
        provider = self._get_provider_from_meta()

        if 'ip_addresses' in params and params['ip_addresses'] is None:
            del params['ip_addresses']

        if 'instance_size' in params:
            if not isinstance(params['instance_size'], float):
                raise ERROR_INVALID_PARAMETER_TYPE(key='instance_size', type='float')

        if 'tags' in params:
            params['tags'] = self._convert_tags_to_dict(params['tags'])

        # Change data through Collector Rule
        if self._is_created_by_collector():
            params = self.collector_rule_mgr.change_cloud_service_data(self.collector_id, domain_id, params)

        cloud_svc_vo: CloudService = self.cloud_svc_mgr.get_cloud_service(cloud_service_id, domain_id)

        if release_project:
            params['project_id'] = None
        elif 'project_id' in params:
            self.identity_mgr.get_project(params['project_id'], domain_id)
        elif secret_project_id and secret_project_id != cloud_svc_vo.project_id:
            params['project_id'] = secret_project_id

        if release_region:
            params.update({
                'region_code': None,
                'ref_region': None
            })
        elif 'region_code' in params:
            params['ref_region'] = self._make_region_key(params, cloud_svc_vo.provider)

        old_cloud_svc_data = dict(cloud_svc_vo.to_dict())

        if 'tags' in params:
            old_tags = old_cloud_svc_data.get('tags', {})
            old_tag_keys = old_cloud_svc_data.get('tag_keys', {})
            new_tags, new_tag_keys = self._convert_tags_to_hash(params['tags'], provider)

            if self._is_different_data(new_tags, old_tags, provider):
                old_tags.update(new_tags)
                old_tag_keys.update(new_tag_keys)
                params['tags'] = old_tags
                params['tag_keys'] = old_tag_keys
            else:
                del params['tags']

        if 'metadata' in params:
            old_metadata = old_cloud_svc_data.get('metadata', {})
            new_metadata = self._convert_metadata(params['metadata'], provider)

            if self._is_different_data(new_metadata, old_metadata, provider):
                old_metadata.update(new_metadata)
                params['metadata'] = old_metadata
            else:
                del params['metadata']

        old_collection_info = old_cloud_svc_data.get('collection_info', [])
        params['collection_info'] = self._get_collection_info(provider, old_collection_info)

        params = self.cloud_svc_mgr.merge_data(params, old_cloud_svc_data)

        cloud_svc_vo = self.cloud_svc_mgr.update_cloud_service_by_vo(params, cloud_svc_vo)

        # Create Update History
        ch_mgr.add_update_history(cloud_svc_vo, params, old_cloud_svc_data)

        # Update Collection History
        state_mgr: CollectionStateManager = self.locator.get_manager('CollectionStateManager')
        state_vo = state_mgr.get_collection_state(cloud_service_id, domain_id)
        if state_vo:
            state_mgr.reset_collection_state(state_vo)
        else:
            state_mgr.create_collection_state(cloud_service_id, 'inventory.CloudService', domain_id)

        if 'project_id' in params:
            note_mgr: NoteManager = self.locator.get_manager('NoteManager')

            # Update Project ID from Notes
            note_vos = note_mgr.filter_notes(cloud_service_id=cloud_service_id, domain_id=domain_id)
            note_vos.update({'project_id': params['project_id']})

        return cloud_svc_vo

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    def delete(self, params):
        self.delete_resource(params)

    @check_required(['cloud_service_id', 'domain_id'])
    def delete_resource(self, params):
        """
        Args:
        params (dict): {
            'cloud_service_id': 'str',
            'domain_id': 'str'
        }
        Returns:
            None
        """
        ch_mgr: ChangeHistoryManager = self.locator.get_manager('ChangeHistoryManager')

        cloud_service_id = params['cloud_service_id']
        domain_id = params['domain_id']

        cloud_svc_vo: CloudService = self.cloud_svc_mgr.get_cloud_service(cloud_service_id, domain_id)

        self.cloud_svc_mgr.delete_cloud_service_by_vo(cloud_svc_vo)

        # Create Update History
        ch_mgr.add_delete_history(cloud_svc_vo)

        # Cascade Delete Collection State
        state_mgr: CollectionStateManager = self.locator.get_manager('CollectionStateManager')
        state_mgr.delete_collection_state_by_resource_id(cloud_service_id, domain_id)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['cloud_service_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            cloud_service_vo (object)

        """

        return self.cloud_svc_mgr.get_cloud_service(params['cloud_service_id'], params['domain_id'],
                                                    only=params.get('only'))

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['cloud_service_id', 'name', 'state', 'account', 'instance_type', 'cloud_service_type',
                          'cloud_service_group', 'provider', 'region_code', 'project_id', 'project_group_id',
                          'domain_id', 'user_projects', 'ip_address'])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def list(self, params):
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',
                    'name': 'str',
                    'state': 'str',
                    'account': 'str',
                    'instance_type': 'str',
                    'cloud_service_type': 'str',
                    'cloud_service_group': 'str',
                    'provider': 'str',
                    'region_code': 'str',
                    'project_id': 'str',
                    'project_group_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)',
                    'user_projects': 'list', // from meta
                    'ip_address': 'str'
                }

        Returns:
            results (list)
            total_count (int)

        """

        query = params.get('query', {})

        return self.cloud_svc_mgr.list_cloud_services(query, change_filter=True, domain_id=params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['options', 'domain_id'])
    def export(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'options': 'list of ExportOptions (spaceone.api.core.v1.ExportOptions)',
                'file_format': 'str',
                'user_projects': 'list', // from meta
            }

        Returns:
            download_url (str): URL to download excel

        """

        domain_id = params['domain_id']
        user_projects = params.get('user_projects')
        options = copy.deepcopy(params['options'])
        file_format = params.get('file_format', 'EXCEL')

        options = self.cloud_svc_mgr.get_export_query_results(options, domain_id, user_projects)
        export_mgr: ExportManager = self.locator.get_manager(ExportManager,
                                                             file_format=file_format,
                                                             file_name='cloud_service_export')

        return export_mgr.export(options, domain_id)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'query.fields', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def analyze(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.AnalyzeQuery)',
                'user_projects': 'list', // from meta
            }

        Returns:
            results (list) : 'list of analyze data'

        """

        query = params.get('query', {})

        return self.cloud_svc_mgr.analyze_cloud_services(query, change_filter=True, domain_id=params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta
            }

        Returns:
            results (list) : 'list of statistics data'

        """

        query = params.get('query', {})

        return self.cloud_svc_mgr.stat_cloud_services(query)

    def _append_resource_group_filter(self, query, domain_id):
        change_filter = []

        for condition in query.get('filter', []):
            key = condition.get('k', condition.get('key'))
            value = condition.get('v', condition.get('value'))
            operator = condition.get('o', condition.get('operator'))

            if key == 'resource_group_id':
                cloud_service_ids = None

                if operator in ['not', 'not_contain', 'not_in', 'not_contain_in']:
                    resource_group_operator = 'not_in'
                else:
                    resource_group_operator = 'in'

                if operator in ['eq', 'not', 'contain', 'not_contain']:
                    cloud_service_ids = self._get_cloud_service_ids_from_resource_group_id(value, domain_id)
                elif operator in ['in', 'not_in', 'contain_in', 'not_contain_in'] and isinstance(value, list):
                    cloud_service_ids = []
                    for v in value:
                        cloud_service_ids += self._get_cloud_service_ids_from_resource_group_id(v, domain_id)

                if cloud_service_ids is not None:
                    change_filter.append({
                        'k': 'cloud_service_id',
                        'v': list(set(cloud_service_ids)),
                        'o': resource_group_operator
                    })

            else:
                change_filter.append(condition)

        query['filter'] = change_filter
        return query

    def _get_cloud_service_ids_from_resource_group_id(self, resource_group_id, domain_id):
        resource_type = 'inventory.CloudService'
        rg_mgr: ResourceGroupManager = self.locator.get_manager('ResourceGroupManager')

        resource_group_filters = rg_mgr.get_resource_group_filter(resource_group_id, resource_type, domain_id,
                                                                  _KEYWORD_FILTER)
        cloud_service_ids = []
        for resource_group_query in resource_group_filters:
            resource_group_query['distinct'] = 'cloud_service_id'
            result = self.cloud_svc_mgr.stat_cloud_services(resource_group_query)
            cloud_service_ids += result.get('results', [])
        return cloud_service_ids

    @staticmethod
    def _make_cloud_service_type_key(resource_data):
        return f'{resource_data["domain_id"]}.{resource_data["provider"]}.' \
               f'{resource_data["cloud_service_group"]}.{resource_data["cloud_service_type"]}'

    @staticmethod
    def _make_region_key(resource_data, provider):
        return f'{resource_data["domain_id"]}.{provider}.{resource_data["region_code"]}'

    @staticmethod
    def _convert_metadata(metadata, provider):
        return {provider: copy.deepcopy(metadata)}

    def _get_collection_info(self, provider, collections: List[CollectionInfo] = None):
        collections = collections or []

        collector_id = self.transaction.get_meta('collector_id')
        secret_id = self.transaction.get_meta('secret.secret_id')
        service_account_id = self.transaction.get_meta('secret.service_account_id')

        new_collection = {
            'provider': provider,
            'collector_id': collector_id,
            'secret_id': secret_id,
            'service_account_id': service_account_id,
            'last_collected_at': datetime.utcnow()
        }

        merged_collections = []
        if collections:
            for collection in collections:
                if collection['provider'] == provider:
                    merged_collections.append(new_collection)
                else:
                    merged_collections.append(collection)
        else:
            merged_collections.append(new_collection)

        return merged_collections

    @staticmethod
    def _convert_tags_to_dict(tags):
        if isinstance(tags, list):
            dot_tags = utils.tags_to_dict(tags)
        elif isinstance(tags, dict):
            dot_tags = copy.deepcopy(tags)
        else:
            dot_tags = {}
        return dot_tags

    @staticmethod
    def _convert_tags_to_hash(dot_tags, provider):
        tag_keys = {provider: list(dot_tags.keys())}

        tags = {provider: {}}
        for key, value in dot_tags.items():
            hashed_key = utils.string_to_hash(key)
            tags[provider][hashed_key] = {'key': key, 'value': value}

        return tags, tag_keys

    @staticmethod
    def _is_different_data(new_data, old_data, provider):
        if new_data[provider] != old_data.get(provider):
            return True
        else:
            return False

    def _get_provider_from_meta(self):
        if self._is_created_by_collector():
            return self.transaction.get_meta('secret.provider')
        else:
            return 'custom'

    def _is_created_by_collector(self):
        return self.collector_id and self.job_id and self.service_account_id and self.plugin_id