import logging
from google.protobuf.json_format import MessageToDict

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.inventory.error import *
from spaceone.inventory.manager.collector_manager import CollectorManager
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.zone_manager import ZoneManager
from spaceone.inventory.manager.pool_manager import PoolManager
from spaceone.inventory.info.collector_info import PluginInfo
from spaceone.inventory.manager.collector_manager.repository_manager import RepositoryManager

_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ['collector_id', 'name', 'provider']


@authentication_handler
@authorization_handler
@event_handler
class CollectorService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)

    @transaction
    @check_required(['name', 'plugin_info', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                'name': 'str',
                'plugin_info': 'dict',
                'priority': 'int',
                'tags': 'list',
                'is_public': 'bool',
                'project_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            collector_vo (object)
        """
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        is_public = params.get('is_public', True)
        project_id = params.get('project_id', None)
        if (is_public is False) and (project_id is None):
            _LOGGER.error(f'[create] project_id is required, if is_public is false')
            raise ERROR_REQUIRED_PARAMETER(key='project_id')

        plugin_info = self._get_plugin(params['plugin_info'], params['domain_id'])
        params['capability'] = plugin_info.get('capability', {})
        params['provider'] = plugin_info.get('provider')
        _LOGGER.debug(f'[create] capability: {params["capability"]}')
        _LOGGER.debug(f'[create] provider: {params["provider"]}')

        return collector_mgr.create_collector(params)

    @transaction
    @check_required(['collector_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            - name
            - priority
            - tags
            - plugin_info(dict)
        """
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        try:
            collector_vo = collector_mgr.get_collector(collector_id, domain_id)
        except Exception as e:
            raise ERROR_NO_COLLECTOR(collector_id=collector_id, domain_id=domain_id)

        # If plugin_info exists, we need deep merge with previous information
        (merged_params, version_check) = self._get_merged_params(params, collector_vo.plugin_info)
        _LOGGER.debug(f'[update] params: {params}')
        _LOGGER.debug(f'[update] merged_params: {merged_params}')

        result = collector_mgr.update_collector_by_vo(collector_vo, merged_params)
        if version_check:
            result = collector_mgr.update_plugin(collector_id, domain_id, merged_params['plugin_info']['version'], None)
        return result

    @transaction
    @check_required(['collector_id', 'domain_id'])
    def delete(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        try:
            # Delete Schedule also
            schedule_mgr = self.locator.get_manager('ScheduleManager')
            schedule_mgr.delete_by_collector_id(collector_id, domain_id)
        except Exception as e:
            _LOGGER.error(f'[delete] failed to delete schedule by collector_id: {collector_id}')

        return collector_mgr.delete_collector(collector_id, domain_id)

    @transaction
    @check_required(['collector_id', 'domain_id'])
    def get(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        only = params.get('only')
        return collector_mgr.get_collector(collector_id, domain_id, only)

    @transaction
    @check_required(['collector_id', 'domain_id'])
    def enable(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        return collector_mgr.enable_collector(collector_id, domain_id)

    @transaction
    @check_required(['collector_id', 'domain_id'])
    def disable(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        return collector_mgr.disable_collector(collector_id, domain_id)

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['collector_id', 'name', 'state', 'priority', 'plugin_id', 'domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(_KEYWORD_FILTER)
    def list(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        query = params.get('query', {})
        return collector_mgr.list_collectors(query)

    @transaction
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @change_tag_filter('tags')
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

    @transaction
    @check_required(['collector_id', 'domain_id'])
    def collect(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        job_info = collector_mgr.collect(params)
        return job_info

    @transaction
    @check_required(['collector_id', 'domain_id'])
    def update_plugin(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        version = params.get('version', None)
        options = params.get('options', None)
        #updated_option = collector_mgr.verify_plugin(collector_id, secret_id, domain_id)
        collector_vo = collector_mgr.update_plugin(collector_id, domain_id, version, options)
        return collector_vo

    @transaction
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

    @transaction
    @check_required(['collector_id', 'schedule', 'domain_id'])
    def add_schedule(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        domain_id = params['domain_id']

        collector_vo = collector_mgr.get_collector(collector_id, domain_id)
        params['collector'] = collector_vo

        scheduler_info = collector_mgr.add_schedule(params)
        return scheduler_info

    @transaction
    @check_required(['collector_id', 'schedule_id', 'domain_id'])
    def get_schedule(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        schedule_id = params['schedule_id']
        domain_id = params['domain_id']
        return collector_mgr.get_schedule(schedule_id, domain_id)


    @transaction
    @check_required(['collector_id', 'schedule_id', 'domain_id'])
    def update_schedule(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        schedule_id = params['schedule_id']
        domain_id = params['domain_id']
        schedule_vo = collector_mgr.get_schedule(schedule_id, domain_id)
        collector_vo = collector_mgr.update_schedule_by_vo(params, schedule_vo)
        return collector_vo

    @transaction
    @check_required(['collector_id', 'schedule_id', 'domain_id'])
    def delete_schedule(self, params):
        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_id = params['collector_id']
        schedule_id = params['schedule_id']
        domain_id = params['domain_id']
        return collector_mgr.delete_schedule(schedule_id, domain_id)

    @transaction
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
            domain_id = params['domain_id']
            # update query
            filter_query.append(_make_query_domain(domain_id))

        # parse schedule
        schedule = params['schedule']
        if 'hour' in schedule:
            # find plugins which has hour rule
            filter_query.append(_make_query_hour(schedule['hour']))

        elif 'minute' in schedule:
            # find pluings which has minute rule
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
        return collector_mgr.list_schedules(query)

    ###############
    # Internal
    ###############
    def _get_merged_params(self, params, plugin_info_vo):
        """ if there is plugin_info at params,
        We need to merge plugin_info with plugin_info_vo
        """
        plugin_info = PluginInfo(plugin_info_vo)
        dict_plugin_info = MessageToDict(plugin_info, preserving_proto_field_name=True)

        #dict_plugin_info = plugin_info_vo.to_dict()
        new_plugin_info = params.get('plugin_info', {})
        # Check version
        db_version = dict_plugin_info['version']
        req_version = new_plugin_info['version']
        version_check = False
        if db_version != req_version:
            # update metadata
            version_check = True

        # new_plugin_info.update(dict_plugin_info)
        dict_plugin_info.update(new_plugin_info)

        new_params = params.copy()
        new_params['plugin_info'] = dict_plugin_info
        return (new_params, version_check)

    def _get_plugin(self, plugin_info, domain_id):
        plugin_id = plugin_info['plugin_id']
        version = plugin_info['version']

        repo_mgr: RepositoryManager = self.locator.get_manager('RepositoryManager')
        plugin_info = repo_mgr.get_plugin(plugin_id, domain_id)
        repo_mgr.check_plugin_version(plugin_id, version, domain_id)

        return plugin_info


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


