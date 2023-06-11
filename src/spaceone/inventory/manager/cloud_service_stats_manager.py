import logging
import copy
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from spaceone.core.manager import BaseManager
from spaceone.core import utils, cache
from spaceone.inventory.error.cloud_service_stats import *
from spaceone.inventory.model.cloud_service_stats_model import CloudServiceStats, MonthlyCloudServiceStats, \
    CloudServiceStatsQueryHistory

_LOGGER = logging.getLogger(__name__)


class CloudServiceStatsManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_stats_model: CloudServiceStats = self.locator.get_model('CloudServiceStats')
        self.monthly_stats_model: MonthlyCloudServiceStats = self.locator.get_model('MonthlyCloudServiceStats')

    def create_cloud_service_stats(self, params, execute_rollback=True):
        def _rollback(cloud_svc_stats_vo: CloudServiceStats):
            _LOGGER.info(f'[create_cloud_service_stats_data._rollback] '
                         f'Delete stats data : {cloud_svc_stats_vo.query_set_id} '
                         f'({cloud_svc_stats_vo.key})')
            cloud_svc_stats_vo.delete()

        cloud_svc_stats_vo: CloudServiceStats = self.cloud_svc_stats_model.create(params)

        if execute_rollback:
            self.transaction.add_rollback(_rollback, cloud_svc_stats_vo)

        return cloud_svc_stats_vo

    def create_monthly_cloud_service_stats(self, params, execute_rollback=True):
        def _rollback(monthly_stats_model: MonthlyCloudServiceStats):
            _LOGGER.info(f'[create_monthly_cloud_service_stats._rollback] '
                         f'Delete stats data : {monthly_stats_model.query_set_id} '
                         f'({monthly_stats_model.key})')
            monthly_stats_model.delete()

        monthly_stats_vo: MonthlyCloudServiceStats = self.monthly_stats_model.create(params)

        if execute_rollback:
            self.transaction.add_rollback(_rollback, monthly_stats_vo)

        return monthly_stats_vo

    def delete_cloud_service_stats(self, query_set_id, domain_id):
        cloud_svc_stats_vos = self.filter_cloud_service_stats(query_set_id=query_set_id, domain_id=domain_id)
        cloud_svc_stats_vos.delete()

    def filter_cloud_service_stats(self, **conditions):
        return self.cloud_svc_stats_model.filter(**conditions)

    def filter_monthly_cloud_service_stats(self, **conditions):
        return self.monthly_stats_model.filter(**conditions)

    def list_cloud_service_stats(self, query):
        return self.cloud_svc_stats_model.query(**query)

    def list_monthly_cloud_service_stats(self, query):
        return self.monthly_stats_model.query(**query)

    # @cache.cacheable(key='inventory:cloud-service-stats:{domain_id}:{query_hash}', expire=3600 * 24)
    def analyze_cloud_service_stats_with_cache(self, query, query_hash, domain_id, target='SECONDARY_PREFERRED'):
        query['target'] = target
        query['date_field'] = 'created_date'
        return self.cloud_svc_stats_model.analyze(**query)

    # @cache.cacheable(key='inventory:monthly-cloud-service-stats:{domain_id}:{query_hash}', expire=3600 * 24)
    def analyze_monthly_cloud_service_stats_with_cache(self, query, query_hash, domain_id, target='SECONDARY_PREFERRED'):
        query['target'] = target
        query['date_field'] = 'created_month'
        return self.monthly_stats_model.analyze(**query)

    def analyze_cloud_service_stats(self, query, domain_id):
        granularity = query['granularity']
        start, end = self._parse_date_range(query)
        del query['start']
        del query['end']

        # Save query history to speed up data loading
        self._create_analyze_query_history(query, granularity, start, end, domain_id)

        # Add date range filter by granularity
        query = self._add_date_range_filter(query, granularity, start, end)
        query_hash = utils.dict_to_hash(query)

        if self._is_monthly_cost(granularity, start, end):
            return self.analyze_monthly_cloud_service_stats_with_cache(query, query_hash, domain_id)
        else:
            return self.analyze_cloud_service_stats_with_cache(query, query_hash, domain_id)

    def stat_cloud_service_stats(self, query):
        return self.cloud_svc_stats_model.stat(**query)

    @cache.cacheable(key='inventory:cloud-service-stats-history:{domain_id}:{query_hash}', expire=600)
    def create_cloud_service_stats_query_history(self, domain_id, query, query_hash, granularity=None,
                                                 start=None, end=None):
        def _rollback(history_vo):
            _LOGGER.info(f'[create_cloud_service_stats_query_history._rollback] Delete query history: {query_hash}')
            history_vo.delete()

        history_model: CloudServiceStatsQueryHistory = self.locator.get_model('CloudServiceStatsQueryHistory')

        history_vos = history_model.filter(query_hash=query_hash, domain_id=domain_id)
        if history_vos.count() == 0:
            history_vo = history_model.create({
                'query_hash': query_hash,
                'query_options': copy.deepcopy(query),
                'granularity': granularity,
                'start': start,
                'end': end,
                'domain_id': domain_id
            })

            self.transaction.add_rollback(_rollback, history_vo)
        else:
            history_vos[0].update({
                'start': start,
                'end': end
            })

    def _create_analyze_query_history(self, query, granularity, start, end, domain_id):
        analyze_query = {
            'group_by': query.get('group_by'),
            'field_group': query.get('field_group'),
            'filter': query.get('filter'),
            'filter_or': query.get('filter_or'),
            'page': query.get('page'),
            'sort': query.get('sort'),
            'fields': query.get('fields'),
            'select': query.get('select'),
        }
        query_hash = utils.dict_to_hash(analyze_query)
        self.create_cloud_service_stats_query_history(domain_id, analyze_query, query_hash, granularity, start, end)

    def _parse_date_range(self, query):
        start_str = query.get('start')
        end_str = query.get('end')
        granularity = query.get('granularity')

        start = self._parse_start_time(start_str)
        end = self._parse_end_time(end_str)

        if start >= end:
            raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                           reason='End date must be greater than start date.')

        if granularity in ['ACCUMULATED', 'MONTHLY']:
            if start + relativedelta(months=12) < end:
                raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                               reason='Request up to a maximum of 12 months.')
        elif granularity == 'DAILY':
            if start + relativedelta(days=31) < end:
                raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                               reason='Request up to a maximum of 31 days.')

        if granularity == 'MONTHLY' and (start.day != 1 or end.day != 1):
            raise ERROR_INVALID_DATE_RANGE(start=start_str, end=end_str,
                                           reason='If the granularity is MONTHLY, '
                                                  'the request must be in YYYY-MM format.')

        return start, end

    def _parse_start_time(self, date_str):
        return self._convert_date_from_string(date_str.strip(), 'start')

    def _parse_end_time(self, date_str):
        date = self._convert_date_from_string(date_str.strip(), 'end')

        if len(date_str) == 7:
            return date + relativedelta(months=1)
        else:
            return date + relativedelta(days=1)

    @staticmethod
    def _convert_date_from_string(date_str, key):
        if len(date_str) == 7:
            # Month (YYYY-MM)
            date_format = '%Y-%m'
        else:
            # Date (YYYY-MM-DD)
            date_format = '%Y-%m-%d'

        try:
            return datetime.strptime(date_str, date_format).date()
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key=key, type=date_format)

    def _add_date_range_filter(self, query, granularity, start: date, end: date):
        query['filter'] = query.get('filter') or []

        if self._is_monthly_cost(granularity, start, end):
            query['filter'].append({
                'k': 'created_month',
                'v': start.strftime('%Y-%m'),
                'o': 'gte'
            })

            query['filter'].append({
                'k': 'created_month',
                'v': end.strftime('%Y-%m'),
                'o': 'lt'
            })
        else:
            query['filter'].append({
                'k': 'created_date',
                'v': start.strftime('%Y-%m-%d'),
                'o': 'gte'
            })

            query['filter'].append({
                'k': 'created_date',
                'v': end.strftime('%Y-%m-%d'),
                'o': 'lt'
            })

        return query

    @staticmethod
    def _is_monthly_cost(granularity, start, end):
        if granularity in ['ACCUMULATED', 'MONTHLY'] and start.day == 1 and end.day == 1:
            return True
        else:
            return False
