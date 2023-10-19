import copy
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

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

        keys, additional_info_keys = self._get_keys_from_query(params['query_options'])
        params['keys'] = keys
        params['additional_info_keys'] = additional_info_keys

        provider = params.get('provider')
        cloud_service_group = params.get('cloud_service_group')
        cloud_service_type = params.get('cloud_service_type')

        if provider and cloud_service_group and cloud_service_type:
            params['ref_cloud_service_type'] = self._make_cloud_service_type_key(params['domain_id'], provider,
                                                                                 cloud_service_group,
                                                                                 cloud_service_type)

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
            keys, additional_info_keys = self._get_keys_from_query(params['query_options'])
            params['keys'] = keys
            params['additional_info_keys'] = additional_info_keys

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
            self._delete_changed_cloud_service_stats(cloud_svc_query_set_vo, created_at)
            self._delete_changed_monthly_cloud_service_stats(cloud_svc_query_set_vo, created_at)
        except Exception as e:
            _LOGGER.error(f'[run_cloud_service_query_set] Failed to save query result: {e}', exc_info=True)
            self._rollback_query_results(cloud_svc_query_set_vo, created_at)
            raise ERROR_CLOUD_SERVICE_QUERY_SET_RUN_FAILED(query_set_id=cloud_svc_query_set_vo.query_set_id)

        self._update_status(cloud_svc_query_set_vo, created_at)
        self._delete_invalid_cloud_service_stats(cloud_svc_query_set_vo)
        self._delete_old_cloud_service_stats(cloud_svc_query_set_vo)
        self._remove_analyze_cache(cloud_svc_query_set_vo.domain_id, cloud_svc_query_set_vo.query_set_id)

    def test_cloud_service_query_set(self, cloud_svc_query_set_vo: CloudServiceQuerySet):
        if cloud_svc_query_set_vo.state == 'DISABLED':
            raise ERROR_CLOUD_SERVICE_QUERY_SET_STATE(state=cloud_svc_query_set_vo.state)

        self.cloud_svc_stats_mgr: CloudServiceStatsManager = self.locator.get_manager('CloudServiceStatsManager')

        _LOGGER.debug(f'[test_cloud_service_query_set] test query set: {cloud_svc_query_set_vo.query_set_id} '
                      f'({cloud_svc_query_set_vo.domain_id})')
        return {
            'results': self._run_analyze_query(cloud_svc_query_set_vo)
        }

    def _run_analyze_query(self, cloud_svc_query_set_vo: CloudServiceQuerySet):
        cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')

        analyze_query = copy.deepcopy(cloud_svc_query_set_vo.query_options)
        provider = cloud_svc_query_set_vo.provider
        cloud_service_group = cloud_svc_query_set_vo.cloud_service_group
        cloud_service_type = cloud_svc_query_set_vo.cloud_service_type
        domain_id = cloud_svc_query_set_vo.domain_id

        analyze_query['filter'] = analyze_query.get('filter', [])
        analyze_query['filter'] += self._make_query_filter(domain_id, provider, cloud_service_group, cloud_service_type)

        analyze_query['group_by'] = analyze_query.get('group_by', []) + _DEFAULT_GROUP_BY

        if 'select' in analyze_query:
            for group_by_key in _DEFAULT_GROUP_BY:
                analyze_query['select'][group_by_key] = group_by_key

        _LOGGER.debug(f'[run_cloud_service_query_set] Run Analyze Query: {analyze_query}')
        response = cloud_svc_mgr.analyze_cloud_services(analyze_query)
        return response.get('results', [])

    def _delete_invalid_cloud_service_stats(self, cloud_svc_query_set_vo: CloudServiceQuerySet):
        domain_id = cloud_svc_query_set_vo.domain_id
        query_set_id = cloud_svc_query_set_vo.query_set_id

        cloud_stats_vos = self.cloud_svc_stats_mgr.filter_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, status='IN_PROGRESS')

        if cloud_stats_vos.count() > 0:
            _LOGGER.debug(f'[_delete_invalid_cloud_service_stats] delete stats count: {cloud_stats_vos.count()}')
            cloud_stats_vos.delete()

        monthly_stats_vos = self.cloud_svc_stats_mgr.filter_monthly_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, status='IN_PROGRESS')

        if monthly_stats_vos.count() > 0:
            _LOGGER.debug(f'[_delete_invalid_cloud_service_stats] delete monthly stats count: {monthly_stats_vos.count()}')
            monthly_stats_vos.delete()

    def _delete_old_cloud_service_stats(self, cloud_svc_query_set_vo: CloudServiceQuerySet):
        now = datetime.utcnow().date()
        query_set_id = cloud_svc_query_set_vo.query_set_id
        domain_id = cloud_svc_query_set_vo.domain_id
        old_created_month = (now - relativedelta(months=12)).strftime('%Y-%m')
        old_created_year = (now - relativedelta(months=36)).strftime('%Y')

        delete_query = {
            'filter': [
                {'k': 'created_month', 'v': old_created_month, 'o': 'lt'},
                {'k': 'query_set_id', 'v': query_set_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'}
            ]
        }

        stats_vos, total_count = self.cloud_svc_stats_mgr.list_cloud_service_stats(delete_query)

        if total_count > 0:
            _LOGGER.debug(f'[delete_old_cloud_service_stats] delete stats count: {total_count}')
            stats_vos.delete()

        monthly_delete_query = {
            'filter': [
                {'k': 'created_year', 'v': old_created_year, 'o': 'lt'},
                {'k': 'query_set_id', 'v': query_set_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'}
            ]
        }

        monthly_stats_vos, total_count = self.cloud_svc_stats_mgr.list_monthly_cloud_service_stats(monthly_delete_query)

        if total_count > 0:
            _LOGGER.debug(f'[_delete_old_cloud_service_stats] delete monthly stats count: {total_count}')
            monthly_stats_vos.delete()

    def _update_status(self, cloud_svc_query_set_vo: CloudServiceQuerySet, created_at):
        domain_id = cloud_svc_query_set_vo.domain_id
        query_set_id = cloud_svc_query_set_vo.query_set_id
        created_date = created_at.strftime('%Y-%m-%d')
        created_month = created_at.strftime('%Y-%m')

        cloud_stats_vos = self.cloud_svc_stats_mgr.filter_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, created_date=created_date, status='IN_PROGRESS')
        cloud_stats_vos.update({'status': 'DONE'})

        monthly_stats_vos = self.cloud_svc_stats_mgr.filter_monthly_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, created_month=created_month, status='IN_PROGRESS')
        monthly_stats_vos.update({'status': 'DONE'})

    def _delete_changed_cloud_service_stats(self, cloud_svc_query_set_vo: CloudServiceQuerySet, created_at):
        domain_id = cloud_svc_query_set_vo.domain_id
        query_set_id = cloud_svc_query_set_vo.query_set_id
        created_date = created_at.strftime('%Y-%m-%d')

        cloud_stats_vos = self.cloud_svc_stats_mgr.filter_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, created_date=created_date, status='DONE')

        _LOGGER.debug(f'[_delete_old_cloud_service_stats] delete count: {cloud_stats_vos.count()}')
        cloud_stats_vos.delete()

    def _delete_changed_monthly_cloud_service_stats(self, cloud_svc_query_set_vo: CloudServiceQuerySet, created_at):
        domain_id = cloud_svc_query_set_vo.domain_id
        query_set_id = cloud_svc_query_set_vo.query_set_id
        created_month = created_at.strftime('%Y-%m')

        monthly_stats_vos = self.cloud_svc_stats_mgr.filter_monthly_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, created_month=created_month, status='DONE')

        _LOGGER.debug(f'[_delete_old_monthly_cloud_service_stats] delete count: {monthly_stats_vos.count()}')
        monthly_stats_vos.delete()

    def _rollback_query_results(self, cloud_svc_query_set_vo: CloudServiceQuerySet, created_at):
        _LOGGER.debug(f'[_rollback_query_results] Rollback Query Results: {cloud_svc_query_set_vo.query_set_id}')
        query_set_id = cloud_svc_query_set_vo.query_set_id
        domain_id = cloud_svc_query_set_vo.domain_id

        cloud_service_stats_vo = self.cloud_svc_stats_mgr.filter_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, created_date=created_at.strftime('%Y-%m-%d'),
            status='IN_PROGRESS')
        cloud_service_stats_vo.delete()

        monthly_stats_vo = self.cloud_svc_stats_mgr.filter_monthly_cloud_service_stats(
            query_set_id=query_set_id, domain_id=domain_id, created_month=created_at.strftime('%Y-%m'),
            status='IN_PROGRESS')
        monthly_stats_vo.delete()

    def _save_query_results(self, query_set_vo: CloudServiceQuerySet, results, created_at):
        for result in results:
            self._save_query_result(result, query_set_vo, created_at)

    def _save_query_result(self, result, query_set_vo: CloudServiceQuerySet, created_at):
        provider = result['provider']
        cloud_service_group = result['cloud_service_group']
        cloud_service_type = result['cloud_service_type']
        region_code = result.get('region_code')
        query_set_id = query_set_vo.query_set_id
        domain_id = query_set_vo.domain_id
        ref_cloud_service_type = self._make_cloud_service_type_key(domain_id, provider, cloud_service_group,
                                                                   cloud_service_type)
        ref_region = self._make_region_key(domain_id, provider, region_code)

        data = {
            'query_set_id': query_set_id,
            'values': {},
            'unit': {},
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
            'created_year': created_at.strftime('%Y'),
            'created_month': created_at.strftime('%Y-%m'),
            'created_date': created_at.strftime('%Y-%m-%d')
        }

        for key in query_set_vo.keys:
            data['values'][key] = result.get(key, 0)

            if key in query_set_vo.unit:
                data['unit'][key] = query_set_vo.unit[key]
            else:
                data['unit'][key] = 'Count'

        for key in query_set_vo.additional_info_keys:
            data['additional_info'][key] = result.get(key)

        self.cloud_svc_stats_mgr.create_cloud_service_stats(data, False)
        self.cloud_svc_stats_mgr.create_monthly_cloud_service_stats(data, False)

    @staticmethod
    def _remove_analyze_cache(domain_id, query_set_id):
        cache.delete_pattern(f'inventory:cloud-service-stats:*:{domain_id}:{query_set_id}:*')
        cache.delete_pattern(f'inventory:stats-query-history:{domain_id}:{query_set_id}:*')

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
    def _make_query_filter(domain_id, provider=None, cloud_service_group=None, cloud_service_type=None):
        _filter = [
            {
                'k': 'domain_id',
                'v': domain_id,
                'o': 'eq'
            }
        ]

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

    @staticmethod
    def _get_keys_from_query(query):
        keys = query.get('fields', {}).keys()
        additional_info_keys = []
        for key in query.get('group_by', []):
            if key not in _DEFAULT_GROUP_BY:
                additional_info_keys.append(key.split('.')[-1:][0])
        return keys, additional_info_keys
