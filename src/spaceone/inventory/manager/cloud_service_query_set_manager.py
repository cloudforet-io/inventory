import copy
import logging
import time
from datetime import datetime

from spaceone.core import cache, utils, queue
from spaceone.core.manager import BaseManager
from spaceone.inventory.error.cloud_service_query_set import *
from spaceone.inventory.model.cloud_service_query_set_model import CloudServiceQuerySet
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager
from spaceone.inventory.manager.cloud_service_stats_manager import CloudServiceStatsManager

_LOGGER = logging.getLogger(__name__)

_DEFAULT_GROUP_BY = ['provider', 'cloud_service_group', 'cloud_service_type', 'region_code', 'account', 'project_id']


class CloudServiceQuerySetManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_query_set_model: CloudServiceQuerySet = self.locator.get_model('CloudServiceQuerySet')
        self.cloud_svc_stats_mgr = None

    @staticmethod
    def push_task(domain_id, system_token):
        task = {
            'name': 'run_query_sets_by_domain',
            'version': 'v1',
            'executionEngine': 'BaseWorker',
            'stages': [{
                'locator': 'SERVICE',
                'name': 'CloudServiceQuerySetService',
                'metadata': {
                    'service': 'inventory',
                    'resource': 'CloudServiceQuerySet',
                    'verb': 'run_query_sets_by_domain',
                    'token': system_token
                },
                'method': 'run_query_sets_by_domain',
                'params': {
                    'params': {
                        'domain_id': domain_id
                    }
                }
            }]
        }

        _LOGGER.debug(f'[push_task] run query sets by domain: {domain_id}')

        queue.put('collector_q', utils.dump_json(task))

    def create_cloud_service_query_set(self, params):
        def _rollback(cloud_svc_query_set_vo: CloudServiceQuerySet):
            _LOGGER.info(
                f'[ROLLBACK] Delete Cloud Service Query Set : {cloud_svc_query_set_vo.name} ({cloud_svc_query_set_vo.query_set_id})')
            cloud_svc_query_set_vo.delete()

        params['query_hash'] = utils.dict_to_hash(params['query_options'])

        _LOGGER.debug(f'[create_cloud_service_query_set] create query set: {params["name"]}')
        cloud_svc_query_set_vo: CloudServiceQuerySet = self.cloud_svc_query_set_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_query_set_vo)

        # Check Analyze Query
        self._run_analyze_query(cloud_svc_query_set_vo)

        return cloud_svc_query_set_vo

    def update_cloud_service_query_set(self, params):
        return self.update_cloud_service_query_set_by_vo(
            params, self.get_cloud_service_query_set(params['query_set_id'], params['domain_id']))

    def update_cloud_service_query_set_by_vo(self, params, cloud_svc_query_set_vo: CloudServiceQuerySet):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("query_set_id")}')
            cloud_svc_query_set_vo.update(old_data)

        if 'query_options' in params:
            params['query_hash'] = utils.dict_to_hash(params['query_options'])

        _LOGGER.debug(f'[update_cloud_service_query_set_by_vo] update query set: {cloud_svc_query_set_vo.query_set_id}')

        self.transaction.add_rollback(_rollback, cloud_svc_query_set_vo.to_dict())

        if 'query_options' in params:
            # Check Analyze Query
            self._run_analyze_query(cloud_svc_query_set_vo)

        return cloud_svc_query_set_vo.update(params)

    def delete_cloud_service_query_set(self, query_set_id, domain_id):
        self.delete_cloud_service_query_set_by_vo(self.get_cloud_service_query_set(query_set_id, domain_id))

    def delete_cloud_service_query_set_by_vo(self, cloud_svc_query_set_vo: CloudServiceQuerySet):
        cloud_svc_stats_mgr: CloudServiceStatsManager = self.locator.get_manager('CloudServiceStatsManager')

        query_set_id = cloud_svc_query_set_vo.query_set_id
        domain_id = cloud_svc_query_set_vo.domain_id

        _LOGGER.debug(f'[delete_cloud_service_query_set_by_vo] delete query set: {query_set_id}')

        # Delete Cloud Service Stats Data
        stats_vos = cloud_svc_stats_mgr.filter_cloud_service_stats(query_set_id=query_set_id, domain_id=domain_id)
        stats_vos.delete()

        # Delete Monthly Cloud Service Stats Data
        monthly_stats_vos = cloud_svc_stats_mgr.filter_monthly_cloud_service_stats(query_set_id=query_set_id,
                                                                                   domain_id=domain_id)
        monthly_stats_vos.delete()

        cloud_svc_query_set_vo.delete()

    def get_cloud_service_query_set(self, query_set_id, domain_id, only=None):
        return self.cloud_svc_query_set_model.get(query_set_id=query_set_id, domain_id=domain_id, only=only)

    def filter_cloud_service_query_sets(self, **conditions):
        return self.cloud_svc_query_set_model.filter(**conditions)

    def list_cloud_service_query_sets(self, query):
        return self.cloud_svc_query_set_model.query(**query)

    def stat_cloud_service_query_sets(self, query):
        return self.cloud_svc_query_set_model.stat(**query)

    def run_cloud_service_query_set(self, cloud_svc_query_set_vo: CloudServiceQuerySet):
        if cloud_svc_query_set_vo.state == 'DISABLED':
            raise ERROR_CLOUD_SERVICE_QUERY_SET_STATE(state=cloud_svc_query_set_vo.state)

        self.cloud_svc_stats_mgr: CloudServiceStatsManager = self.locator.get_manager('CloudServiceStatsManager')

        _LOGGER.debug(f'[run_cloud_service_query_set] run query set: {cloud_svc_query_set_vo.query_set_id} '
                      f'({cloud_svc_query_set_vo.domain_id})')
        results = self._run_analyze_query(cloud_svc_query_set_vo)

        created_at = datetime.utcnow()

        try:
            self._save_query_results(cloud_svc_query_set_vo, results, created_at)
            self._delete_old_cloud_service_stats(cloud_svc_query_set_vo, created_at)
            self._delete_old_monthly_cloud_service_stats(cloud_svc_query_set_vo, created_at)
        except Exception as e:
            _LOGGER.error(f'[run_cloud_service_query_set] Failed to save query result: {e}', exc_info=True)
            self._rollback_query_results(cloud_svc_query_set_vo, created_at)
            raise ERROR_CLOUD_SERVICE_QUERY_SET_RUN_FAILED(query_set_id=cloud_svc_query_set_vo.query_set_id)

        self._remove_analyze_cache(cloud_svc_query_set_vo.domain_id)

    def _run_analyze_query(self, cloud_svc_query_set_vo: CloudServiceQuerySet):
        cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')

        analyze_query = copy.deepcopy(cloud_svc_query_set_vo.query_options)
        provider = cloud_svc_query_set_vo.provider
        cloud_service_group = cloud_svc_query_set_vo.cloud_service_group
        cloud_service_type = cloud_svc_query_set_vo.cloud_service_type

        analyze_query['filter'] = analyze_query.get('filter', [])
        analyze_query['filter'] += self._make_query_filter(provider, cloud_service_group, cloud_service_type)

        analyze_query['group_by'] = analyze_query.get('group_by', []) + _DEFAULT_GROUP_BY

        if 'select' in analyze_query:
            for group_by_key in _DEFAULT_GROUP_BY:
                analyze_query['select'][group_by_key] = group_by_key

        _LOGGER.debug(f'[run_cloud_service_query_set] Run Analyze Query: {analyze_query}')
        response = cloud_svc_mgr.analyze_cloud_services(analyze_query)
        return response.get('results', [])

    def _delete_old_cloud_service_stats(self, cloud_svc_query_set_vo: CloudServiceQuerySet, created_at):
        domain_id = cloud_svc_query_set_vo.domain_id
        query_set_id = cloud_svc_query_set_vo.query_set_id
        created_date = created_at.strftime('%Y-%m-%d')
        timestamp = int(time.mktime(created_at.timetuple()))

        query = {
            'filter': [
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'query_set_id', 'v': query_set_id, 'o': 'eq'},
                {'k': 'created_date', 'v': created_date, 'o': 'eq'},
                {'k': 'timestamp', 'v': timestamp, 'o': 'not'}
            ]
        }

        _LOGGER.debug(f'[_delete_old_cloud_service_stats] Query: {query}')
        cloud_stats_vos, total_count = self.cloud_svc_stats_mgr.list_cloud_service_stats(query)
        cloud_stats_vos.delete()

    def _delete_old_monthly_cloud_service_stats(self, cloud_svc_query_set_vo: CloudServiceQuerySet, created_at):
        domain_id = cloud_svc_query_set_vo.domain_id
        query_set_id = cloud_svc_query_set_vo.query_set_id
        created_month = created_at.strftime('%Y-%m')
        timestamp = int(time.mktime(created_at.timetuple()))

        query = {
            'filter': [
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'query_set_id', 'v': query_set_id, 'o': 'eq'},
                {'k': 'created_month', 'v': created_month, 'o': 'eq'},
                {'k': 'timestamp', 'v': timestamp, 'o': 'not'}
            ]
        }

        _LOGGER.debug(f'[_delete_old_monthly_cloud_service_stats] Query: {query}')
        monthly_stats_vos, total_count = self.cloud_svc_stats_mgr.list_monthly_cloud_service_stats(query)
        monthly_stats_vos.delete()

    def _rollback_query_results(self, cloud_svc_query_set_vo: CloudServiceQuerySet, created_at):
        _LOGGER.debug(f'[_rollback_query_results] Rollback Query Results: {cloud_svc_query_set_vo.query_set_id}')
        query_set_id = cloud_svc_query_set_vo.query_set_id
        domain_id = cloud_svc_query_set_vo.domain_id
        timestamp = int(time.mktime(created_at.timetuple()))

        cloud_service_stats_vo = self.cloud_svc_stats_mgr.filter_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, timestamp=timestamp)
        cloud_service_stats_vo.delete()

        monthly_stats_vo = self.cloud_svc_stats_mgr.filter_monthly_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, timestamp=timestamp)
        monthly_stats_vo.delete()

    def _save_query_results(self, cloud_svc_query_set_vo: CloudServiceQuerySet, results, created_at):
        query_set_id = cloud_svc_query_set_vo.query_set_id
        domain_id = cloud_svc_query_set_vo.domain_id
        analyze_query = cloud_svc_query_set_vo.query_options
        unit = cloud_svc_query_set_vo.unit
        original_group_by = set(analyze_query.get('group_by', [])) - set(_DEFAULT_GROUP_BY)
        timestamp = int(time.mktime(created_at.timetuple()))

        for result in results:
            self._save_query_result(result, query_set_id, original_group_by, unit, domain_id, created_at, timestamp)

    @staticmethod
    def _remove_analyze_cache(domain_id):
        cache.delete_pattern(f'inventory:cloud-service-stats:{domain_id}:*')
        cache.delete_pattern(f'inventory:monthly-cloud-service-stats:{domain_id}:*')

    def _save_query_result(self, result, query_set_id, original_group_by, unit, domain_id, created_at, timestamp):
        provider = result['provider']
        cloud_service_group = result['cloud_service_group']
        cloud_service_type = result['cloud_service_type']
        region_code = result.get('region_code')
        ref_cloud_service_type = self._make_cloud_service_type_key(domain_id, provider, cloud_service_group,
                                                                   cloud_service_type)
        ref_region = self._make_region_key(domain_id, provider, region_code)

        data = {
            'query_set_id': query_set_id,
            'provider': provider,
            'cloud_service_group': cloud_service_group,
            'cloud_service_type': cloud_service_type,
            'ref_cloud_service_type': ref_cloud_service_type,
            'region_code': region_code,
            'ref_region': ref_region,
            'account': result.get('account'),
            'project_id': result.get('project_id'),
            'domain_id': domain_id,
            'additional_info': {},
            'created_at': created_at,
            'timestamp': timestamp,
            'created_year': created_at.strftime('%Y'),
            'created_month': created_at.strftime('%Y-%m'),
            'created_date': created_at.strftime('%Y-%m-%d')
        }

        group_by_keys = []
        for key in original_group_by:
            group_by_key = key.rsplit('.', 1)[-1]
            data['additional_info'][group_by_key] = result.get(group_by_key)
            group_by_keys.append(group_by_key)

        field_keys = set(result.keys()) - set(group_by_keys) - set(_DEFAULT_GROUP_BY)
        for key in field_keys:
            field_data = copy.deepcopy(data)
            field_data['key'] = key
            field_data['value'] = result.get(key)
            field_data['unit'] = unit.get(key, 'Count')

            self.cloud_svc_stats_mgr.create_cloud_service_stats(field_data, False)
            self.cloud_svc_stats_mgr.create_monthly_cloud_service_stats(field_data, False)

    @staticmethod
    def _make_cloud_service_type_key(domain_id, provider, cloud_service_group, cloud_service_type):
        return f'{domain_id}.{provider}.{cloud_service_group}.{cloud_service_type}'

    @staticmethod
    def _make_region_key(domain_id, provider, region_code):
        if region_code:
            return f'{domain_id}.{provider}.{region_code}'
        else:
            return None

    @staticmethod
    def _make_query_filter(provider=None, cloud_service_group=None, cloud_service_type=None):
        _filter = []

        if provider:
            _filter.append({
                'k': 'provider',
                'v': provider,
                'o': 'eq'
            })

        if cloud_service_group:
            _filter.append({
                'k': 'cloud_service_group',
                'v': cloud_service_group,
                'o': 'eq'
            })

        if cloud_service_type:
            _filter.append({
                'k': 'cloud_service_type',
                'v': cloud_service_type,
                'o': 'eq'
            })

        return _filter
