# -*- coding: utf-8 -*-

import logging
import json

from jsonschema import validate
from datetime import datetime

from spaceone.core import queue
from spaceone.core import config
from spaceone.core.token import get_token
from spaceone.core.error import *
from spaceone.core.manager import BaseManager
from spaceone.core.scheduler.task_schema import SPACEONE_TASK_SCHEMA

from spaceone.inventory.error import *

from spaceone.inventory.manager.collector_manager.collecting_manager import CollectingManager
from spaceone.inventory.manager.collector_manager.collector_db import CollectorDB
from spaceone.inventory.manager.collector_manager.filter_manager import FilterManager
from spaceone.inventory.manager.collector_manager.job_manager import JobManager
from spaceone.inventory.manager.collector_manager.plugin_manager import PluginManager
from spaceone.inventory.manager.collector_manager.schedule_manager import ScheduleManager
from spaceone.inventory.manager.collector_manager.secret_manager import SecretManager
from spaceone.inventory.manager.collector_manager.repository_manager import RepositoryManager

__ALL__ = ['CollectorManager', 'CollectingManager', 'CollectorDB', 'FilterManager', 'PluginManager',
           'ScheduleManager', 'JobManager', 'SecretManager']

_LOGGER = logging.getLogger(__name__)

class CollectorManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collector_db = self.locator.get_manager('CollectorDB')

    def create_collector(self, params):
        """
        Args: params
          - name
          - plugin_info
          - state
          - priority
          - tags
          - domain_id
          - is_public
          - project_id
        """
        # Create DB first
        collector_vo = self.collector_db.create_collector(params)

        # Plugin Manager
        plugin_mgr = self.locator.get_manager('PluginManager')

        # init plugin
        try:
            #updated_params = plugin_mgr.verify(params)
            plugin_metadata = plugin_mgr.init(params)
            _LOGGER.debug(f'[create_collector] metadata: {plugin_metadata}')
            plugin_info = params.get('plugin_info', {})
            #plugin_info['options'] = updated_params['options']
            plugin_info['metadata'] = plugin_metadata['metadata']
            params2 = {'plugin_info': plugin_info}
            collector_vo = self.update_collector_by_vo(collector_vo, params2)
            return collector_vo
        except Exception as e:
            _LOGGER.debug(f'[create_collector] failed plugin init: {e}')
            raise ERROR_VERIFY_PLUGIN_FAILURE(params=params)

    def update_plugin(self, collector_id, domain_id, version, options):
        collector_vo = self.get_collector(collector_id, domain_id)
        collector_dict = collector_vo.to_dict()
        plugin_info = collector_dict['plugin_info']
        if version:
            # Update plugin_version
            plugin_id = plugin_info['plugin_id']
            repo_mgr = self.locator.get_manager('RepositoryManager')
            repo_mgr.check_plugin_version(plugin_id, version, domain_id)

            plugin_info['version'] = version
        if options:
            # Overwriting
            plugin_info['options'] = options
        params = {
            'plugin_info': plugin_info
        }
        _LOGGER.debug(f'[update_plugin] {plugin_info}')
        return self.update_collector_by_vo(collector_vo, params)

    def verify_plugin(self, collector_id, secret_id, domain_id):
        # Get collector
        collector = self.get_collector(collector_id, domain_id)
        collector_dict = collector.to_dict()
        plugin_info = collector_dict['plugin_info']
        new_plugin_info = plugin_info.copy()

        # Call Plugin Manager
        plugin_mgr = self.locator.get_manager('PluginManager')
        _LOGGER.debug(f'[verify_plugin] secret_id: {secret_id}')
        try:
            return plugin_mgr.verify_by_plugin_info(new_plugin_info, domain_id, secret_id)
        except Exception as e:
            _LOGGER.debug(f'[verify_plugin] failed plugin verify: {e}')
            raise ERROR_VERIFY_PLUGIN_FAILURE(params={'collector_id': collector_id,
                                                        'secret_id': secret_id})

    def delete_collector(self, collector_id, domain_id):
        # Cascade Delete (Job, Schedule)
        # Delete Related Job
        try:
            job_mgr = self.locator.get_manager('JobManager')
            job_mgr.delete_by_collector_id(collector_id, domain_id)
        except Exception as e:
            _LOGGER.error(f'[delete_collector] fail to delete job, collector_id: {collector_id}, {e}')

        try:
            schedule_mgr = self.locator.get_manager('ScheduleManager')
            schedule_mgr.delete_by_collector_id(collector_id, domain_id)
        except Exception as e:
            _LOGGER.error(f'[delete_collector] fail to delete schedule, collector_id: {collector_id}, {e}')

        self.collector_db.delete_collector(collector_id=collector_id, domain_id=domain_id)

    def get_collector(self, collector_id, domain_id, only=None):
        return self.collector_db.get_collector(collector_id=collector_id, domain_id=domain_id, only=only)

    def update_collector_by_vo(self, collector_vo, params):
        """ Update collector
        Get collector_vo, then update with this
        """
        return collector_vo.update(params)

    def enable_collector(self, collector_id, domain_id):
        return self.collector_db.enable_collector(collector_id=collector_id, domain_id=domain_id)

    def disable_collector(self, collector_id, domain_id):
        return self.collector_db.disable_collector(collector_id=collector_id, domain_id=domain_id)

    def list_collectors(self, query):
        return self.collector_db.list_collectors(query)

    def stat_collectors(self, query):
        return self.collector_db.stat_collectors(query)

    def collect(self, params):
        """
        Args:
            params: {
                'collector_id': str
                'filter': dict
                'secret_id': str
                'collect_mode': str
                'use_cache': bool
                'domain_id': str
            }
        """
        collector_id = params['collector_id']
        domain_id = params['domain_id']
        collect_mode = params.get('collect_mode', 'ALL')

        collector_vo = self.get_collector(collector_id, domain_id)
        collector_dict = collector_vo.to_dict()
        # TODO: get Queue from config

        # Create Job
        job_mgr = self.locator.get_manager('JobManager')
        created_job = job_mgr.create_job(collector_vo, params)

        # Make in-progress
        try:
            job_mgr.make_inprgress(created_job.job_id, domain_id)
        except Exception as e:
            _LOGGER.debug(f'[collect] {e}')
            _LOGGER.debug(f'[collect] fail to change {collector_id} job state to in-progress')

        # Create Pipeline & Push
        try:
            secret_id = params.get('secret_id', None)
            plugin_mgr = self.locator.get_manager('PluginManager')
            secret_list = plugin_mgr.get_secrets_from_plugin_info(
                                                        collector_dict['plugin_info'],
                                                        domain_id,
                                                        secret_id
                                                    )
            _LOGGER.debug(f'[collector] number of secret: {len(secret_list)}')
        except Exception as e:
            _LOGGER.debug(f'[collect] failed in Secret Patch stage: {e}')
            job_mgr.make_failure(created_job.job_id, domain_id)
            raise ERROR_COLLECT_INITIALIZE(stage='Secret Patch', params={params})

        # Apply Filter Format
        try:
            filter_mgr = self.locator.get_manager('FilterManager')
            filters = params.get('filter', {})
            plugin_info = collector_dict['plugin_info']
            collect_filter, secret_list = filter_mgr.get_collect_filter(filters,
                                                                        plugin_info,
                                                                        secret_list)
            _LOGGER.debug(f'[collector] number of secret after filter transform: {len(secret_list)}')
        except Exception as e:
            _LOGGER.debug(f'[collect] failed on Filter Transform stage: {e}')
            job_mgr.make_failure(created_job.job_id, domain_id)
            raise ERROR_COLLECT_INITIALIZE(stage='Filter Format', params={params})

        # Loop all secret_list
        for secret_id in secret_list:
            # Do collect per secret
            try:
                # TODO:
                # Make Pipeline, then push
                # parameter of pipeline
                req_params = self._make_collecting_parameters(collector_dict=collector_dict,
                                                              secret_id=secret_id,
                                                              domain_id=domain_id,
                                                              job_vo=created_job,
                                                              collect_filter=collect_filter,
                                                              params=params
                                                              )
                _LOGGER.debug(f'[collect] params for collecting: {req_params}')
                job_mgr.increase_remained_tasks(created_job.job_id, domain_id)

                # TODO: Push to Queue
                # Make SpaceONE Template Pipeline
                task = self._create_task(req_params, domain_id)
                queue_name = self._get_queue_name(name='collect_queue')

                if task and queue_name:
                    # Push to queue
                    _LOGGER.debug('####### Asynchronous collect ########')
                    validate(task, schema=SPACEONE_TASK_SCHEMA)
                    json_task = json.dumps(task)
                    queue.put(queue_name, json_task)
                else:
                    # Do synchronus collect
                    _LOGGER.debug('####### Synchronous collect ########')
                    collecting_mgr = self.locator.get_manager('CollectingManager')
                    collecting_mgr.collecting_resources(**req_params)

            except Exception as e:
                # Do not exit, just book-keeping
                _LOGGER.error(f'[collect] collecting failed with {secret_id}: {e}')

        # Update Timestamp
        self._update_last_collected_time(collector_vo.collector_id, domain_id)
        return created_job

    def _update_last_collected_time(self, collector_id, domain_id):
        """ Update last_updated_time of collector
        """
        collector_vo = self.get_collector(collector_id, domain_id)
        params = {'last_collected_at': datetime.utcnow()}
        self.update_collector_by_vo(collector_vo, params)

    def _get_queue_name(self, name='collect_queue'):
        """ Return queue
        """
        try:
            return config.get_global(name)
        except Exception as e:
            _LOGGER.warning(f'[_get_queue_name] name: {name} is not configured')
            return None

    def _create_task(self, req_params, domain_id):
        """ Create Pipeline Task
        """
        try:
            task = {
                'locator': 'MANAGER',
                'name': 'CollectingManager',
                'metadata': {'token': get_token(), 'domain_id': domain_id},
                'method': 'collecting_resources',
                'params': req_params
            }
            stp = {'name': 'collecting_resources',
                   'version': 'v1',
                   'executionEngine': 'BaseWorker',
                   'stages': [task]
                   }
            _LOGGER.debug(f'[_create_task] tasks: {stp}')
            return stp
        except Exception as e:
            _LOGGER.warning(f'[_create_task] failed asynchronous collect, {e}')
            return None

    def _make_collecting_parameters(self, **kwargs):
        """ Make parameters for collecting_resources

        Args:
            collector_dict
            secret_id
            domain_id
            filter
            job_vo
            collect_filter
            params

        """

        new_params = {
            'secret_id': kwargs['secret_id'],
            'job_id':    kwargs['job_vo'].job_id,
            'filters':   kwargs['collect_filter'],
            'domain_id': kwargs['domain_id'],
            'collector_id': kwargs['collector_dict']['collector_id']
        }

        # plugin_info dict
        new_params.update({'plugin_info': kwargs['collector_dict']['plugin_info'].to_dict()})

        # use_cache
        params = kwargs['params']
        use_cache = params.get('use_cache', False)
        new_params.update({'use_cache': use_cache})

        _LOGGER.debug(f'[_make_collecting_parameters] params: {new_params}')
        return new_params

    def add_schedule(self, params):
        schedule_mgr = self.locator.get_manager('ScheduleManager')
        return schedule_mgr.create_schedule(params)

    def get_schedule(self, schedule_id, domain_id):
        schedule_mgr = self.locator.get_manager('ScheduleManager')
        return schedule_mgr.get_schedule(schedule_id, domain_id)


    def list_schedules(self, query):
        schedule_mgr = self.locator.get_manager('ScheduleManager')
        return schedule_mgr.list_schedules(query)

    def delete_schedule(self, schedule_id, domain_id):
        schedule_mgr = self.locator.get_manager('ScheduleManager')
        schedule_mgr.delete_schedule(schedule_id, domain_id)

    def update_schedule_by_vo(self, params, schedule_vo):
        params = self._check_filter(params)

        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Scheduler Data : {old_data["schedule_id"]}')
            schedule_vo.update(old_data)

        self.transaction.add_rollback(_rollback, schedule_vo.to_dict())
        return schedule_vo.update(params)

