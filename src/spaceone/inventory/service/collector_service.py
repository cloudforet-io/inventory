import logging
from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.core import utils
from spaceone.inventory.error import *
from spaceone.inventory.manager.collection_state_manager import CollectionStateManager
from spaceone.inventory.manager.collector_manager import CollectorManager
from spaceone.inventory.manager.collector_manager.repository_manager import RepositoryManager

_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ['collector_id', 'name', 'provider']


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CollectorService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'plugin_info', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                'name': 'str',
                'plugin_info': 'dict',
                'provider': 'str',
                'schedule': 'dict',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            collector_vo (object)
        """

        if 'tags' in params:
            if isinstance(params['tags'], list):
                params['tags'] = utils.tags_to_dict(params['tags'])

        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        plugin_info = self._get_plugin(params['plugin_info'], params['domain_id'])
        params['capability'] = plugin_info.get('capability', {})
        params['provider'] = self._get_plugin_providers(params.get('provider'), plugin_info)

        # Check schedule type
        if 'schedule' in params:
            collector_mgr.is_supported_schedule(params['plugin_info'], params['schedule'])

        _LOGGER.debug(f'[create] capability: {params["capability"]}')
        _LOGGER.debug(f'[create] provider: {params["provider"]}')

        return collector_mgr.create_collector(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                'collector_id': 'str',
                'name': 'str',
                'priority': 'int',
                'tags': 'dict',
                'schedule': 'dict',
                'domain_id': 'str'
            }

        Returns:
            collector_vo (object)
        """

        if 'tags' in params:
            if isinstance(params['tags'], list):
                params['tags'] = utils.tags_to_dict(params['tags'])

        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_vo = collector_mgr.get_collector(params['collector_id'], params['domain_id'])

        # Check schedule type
        if 'schedule' in params:
            collector_dict = collector_vo.to_dict()
            collector_mgr.is_supported_schedule(collector_dict.get('plugin_info', {}), params['schedule'])

        return collector_mgr.update_collector_by_vo(collector_vo, params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'domain_id'])
    def delete(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')

        collector_mgr.delete_collector(params['collector_id'], params['domain_id'])

        # Cascade Delete Collection State
        state_mgr: CollectionStateManager = self.locator.get_manager('CollectionStateManager')
        state_mgr.delete_collection_state_by_collector_id(params['collector_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'domain_id'])
    def get(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        only = params.get('only')
        return collector_mgr.get_collector(collector_id, domain_id, only)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'domain_id'])
    def enable(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        return collector_mgr.enable_collector(collector_id, domain_id)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'domain_id'])
    def disable(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        return collector_mgr.disable_collector(collector_id, domain_id)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['collector_id', 'name', 'state', 'priority', 'plugin_id', 'domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    def list(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        query = params.get('query', {})
        return collector_mgr.list_collectors(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        query = params.get('query', {})
        return collector_mgr.stat_collectors(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'domain_id'])
    def collect(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        job_info = collector_mgr.collect(params)
        return job_info

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'domain_id'])
    def update_plugin(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        version = params.get('version')
        options = params.get('options')
        upgrade_mode = params.get('upgrade_mode')
        #updated_option = collector_mgr.verify_plugin(collector_id, secret_id, domain_id)
        collector_vo = collector_mgr.update_plugin(collector_id, domain_id, version, options, upgrade_mode)

        return collector_vo

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'domain_id'])
    def verify_plugin(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        secret_id = params.get('secret_id', None)
        domain_id = params['domain_id']
        #updated_option = collector_mgr.verify_plugin(collector_id, secret_id, domain_id)
        collector_mgr.verify_plugin(collector_id, secret_id, domain_id)
        # If you here, succeed verify
        #return {'status': True}

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'schedule', 'domain_id'])
    def add_schedule(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']

        collector_vo = collector_mgr.get_collector(collector_id, domain_id)
        params['collector'] = collector_vo

        collector_dict = collector_vo.to_dict()
        plugin_info = collector_dict['plugin_info']

        # Check schedule type
        collector_mgr.is_supported_schedule(plugin_info, params['schedule'])

        scheduler_info = collector_mgr.add_schedule(params)
        return scheduler_info

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'schedule_id', 'domain_id'])
    def get_schedule(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        schedule_id = params['schedule_id']
        domain_id = params['domain_id']
        return collector_mgr.get_schedule(schedule_id, domain_id)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'schedule_id', 'domain_id'])
    def update_schedule(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        schedule_id = params['schedule_id']
        domain_id = params['domain_id']
        schedule_vo = collector_mgr.get_schedule(schedule_id, domain_id)
        collector_vo = collector_mgr.update_schedule_by_vo(params, schedule_vo)
        return collector_vo

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'schedule_id', 'domain_id'])
    def delete_schedule(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        schedule_id = params['schedule_id']
        domain_id = params['domain_id']
        return collector_mgr.delete_schedule(schedule_id, domain_id)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'domain_id'])
    @change_only_key({'collector_info': 'collector'}, key_path='query.only')
    @append_query_filter(['collector_id', 'schedule_id', 'domain_id'])
    @append_keyword_filter(['schedule_id', 'name'])
    def list_schedules(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        query = params.get('query', {})

        # Temporary code for DB migration
        if 'only' in query:
            query['only'] += ['collector_id']

        return collector_mgr.list_schedules(query)

    ############################
    # for schedule
    ############################
    @check_required(['schedule'])
    def scheduled_collectors(self, params):
        """ Search all collectors in this schedule

        This is global search out-of domain
        Args:
            schedule(dict): {
                    'hours': list,
                    'minutes': list
                }

            domain_id: optional

        ex) {'hour': 3}

        Returns: collectors_info
        """
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')

        # state: ENABLED
        # filter_query = [{'k':'collector.state','v':'ENABLED','o':'eq'}]
        filter_query = []

        if 'domain_id' in params:
            filter_query.append(_make_query_domain(params['domain_id']))

        # parse schedule
        schedule = params['schedule']
        if 'hour' in schedule:
            # find plugins which has hour rule
            filter_query.append(_make_query_hour(schedule['hour']))

        elif 'minute' in schedule:
            # find plugins which has minute rule
            filter_query.append(_make_query_minute(schedule['minute']))

        elif 'interval' in schedule:
            # find interval schedules
            filter_query.append(_make_query_interval())
        else:
            # TODO: CRON
            pass

        # make query for list_collector
        query = {'filter': filter_query}

        _LOGGER.debug(f'[scheduled_collectors] query: {query}')
        # return collector_mgr.list_schedules(query)
        return collector_mgr.list_collectors(query)

    def _get_plugin(self, plugin_info, domain_id):
        plugin_id = plugin_info['plugin_id']
        # version = plugin_info['version']

        repo_mgr: RepositoryManager = self.locator.get_manager('RepositoryManager')
        plugin_info = repo_mgr.get_plugin(plugin_id, domain_id)
        # repo_mgr.check_plugin_version(plugin_id, version, domain_id)

        return plugin_info

    @staticmethod
    def _get_plugin_providers(provider, plugin_info):
        supported_providers = plugin_info.get('capability', {}).get('supported_providers', [])

        if supported_providers:
            # Multi providers
            if provider in supported_providers:
                return provider
            else:
                raise ERROR_INVALID_PARAMETER(key='provider', reason=f'Not supported provider: {provider}')
        else:
            # Single provider
            return provider if provider else plugin_info.get('provider')

def _make_query_domain(domain_id):
    return {
        'k': 'domain_id',
        'v': domain_id,
        'o': 'eq'
        }


def _make_query_hour(hour: int):
    # make query hour
    return {
        'k': 'schedule.hours',
        'v': hour,
        'o': 'contain'
        }


def _make_query_minute(minute: int):
    # make minute query
    return {
        'k': 'schedule.minute',
        'v': minute,
        'o': 'contain'
        }


def _make_query_interval():
    return {
        'k': 'schedule.interval',
        'v': 0,
        'o': 'gt'
        }
