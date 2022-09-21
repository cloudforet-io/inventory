import logging
import json
import time

from google.protobuf.json_format import MessageToDict

from spaceone.core import config, cache
from spaceone.core import queue
from spaceone.core.error import *
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.inventory.manager.collector_manager.job_manager import JobManager
from spaceone.inventory.manager.collector_manager.job_task_manager import JobTaskManager
from spaceone.inventory.error import *
from spaceone.inventory.lib import rule_matcher

_LOGGER = logging.getLogger(__name__)

######################################################################
#    ************ Very Important ************
#
# This is resource map for collector
# If you add new service and manager for specific RESOURCE_TYPE,
# add here for collector
######################################################################
RESOURCE_MAP = {
    'inventory.Server': 'ServerManager',
    'inventory.FilterCache': 'FilterManager',
    'inventory.CloudService': 'CloudServiceManager',
    'inventory.CloudServiceType': 'CloudServiceTypeManager',
    'inventory.Region': 'RegionManager',
    'inventory.ErrorResource': 'CollectingManager',
}

SERVICE_MAP = {
    'inventory.Server': 'ServerService',
    'inventory.FilterCache': 'CollectorService',
    'inventory.CloudService': 'CloudServiceService',
    'inventory.CloudServiceType': 'CloudServiceTypeService',
    'inventory.Region': 'RegionService',
    'inventory.ErrorResource': 'CollectorService',
}

DB_QUEUE_NAME = 'db_q'
NOT_COUNT = 0
CREATED = 1
UPDATED = 2
ERROR = 3
JOB_TASK_STAT_EXPIRE_TIME = 3600  # 1 hour
WATCHDOG_WAITING_TIME = 30  # wait 30 seconds, before watchdog works


#################################################
# Collecting Resource and Update DB
#################################################

class CollectingManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.secret = None  # secret info for update meta
        self.use_db_queue = False
        self.initialize()
        self.job_mgr: JobManager = self.locator.get_manager('JobManager')
        self.job_task_mgr: JobTaskManager = self.locator.get_manager('JobTaskManager')
        self.db_queue = DB_QUEUE_NAME

    def initialize(self):
        _LOGGER.debug(f'[initialize] initialize Worker configuration')
        queues = config.get_global('QUEUES', {})
        if DB_QUEUE_NAME in queues:
            self.use_db_queue = True

        _LOGGER.debug(f'[initialize] use db_queue: {self.use_db_queue}')

    ##########################################################
    # collect
    #
    # links: https://pyengine.atlassian.net/wiki/spaces/CLOUD/pages/682459145/3.+Collector+Rule+Management
    #
    ##########################################################
    def collecting_resources(self, plugin_info, secret_id, filters, domain_id, **kwargs):
        """ This is single call of real plugin with endpoint

        All parameter should be primitive type(Json), not object.
        Because this method will be executed by worker.
        Args:
            plugin_info(dict)
            kwargs: {
                'job_id': 'str',
                'use_cache': bool
            }
        """

        # Check Job State first, if job state is canceled, stop process
        job_task_id = kwargs['job_task_id']
        job_id = kwargs['job_id']
        collector_id = kwargs['collector_id']

        if self.job_mgr.should_cancel(job_id, domain_id):
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                'ERROR_COLLECT_CANCELED',
                'The job has been canceled.'
            )
            self.job_task_mgr.make_canceled(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            raise ERROR_COLLECT_CANCELED(job_id=job_id)

        if self.job_task_mgr.check_duplicate_job_tasks(collector_id, secret_id, domain_id):
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                'ERROR_DUPLICATE_JOB',
                'A duplicate job is already running.'
            )
            self.job_task_mgr.make_canceled(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            raise ERROR_COLLECT_CANCELED(job_id=job_id)

        # Create proper connector
        connector = self._get_connector(plugin_info, domain_id)

        collect_filter = filters
        try:
            # use_cache
            use_cache = kwargs['use_cache']
            if use_cache:
                key = f'collector-filter:{collector_id}:{secret_id}'
                value = cache.get(key)
                _LOGGER.debug(f'[collecting_resources] cache -> {key}: {value}')
                if value:
                    collect_filter.update(value)
            else:
                _LOGGER.debug(f'[collecting_resources] no cache mode')

        except Exception as e:
            _LOGGER.debug(f'[collecting_resources] cache error,{e}')

        try:
            secret_mgr = self.locator.get_manager('SecretManager')
            secret_data = secret_mgr.get_secret_data(secret_id, domain_id)
            self.secret = secret_mgr.get_secret(secret_id, domain_id)

        except ERROR_BASE as e:
            _LOGGER.error(f'[collecting_resources] fail to get secret_data: {secret_id}')
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                e.error_code,
                e.message,
                {'resource_type': 'secret.Secret', 'resource_id': secret_id}
            )
            self.job_task_mgr.make_failure(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            raise ERROR_COLLECTOR_SECRET(plugin_info=plugin_info, param=secret_id)

        except Exception as e:
            _LOGGER.error(f'[collecting_resources] fail to get secret_data: {secret_id}')
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                'ERROR_COLLECTOR_SECRET',
                e,
                {'resource_type': 'secret.Secret', 'resource_id': secret_id}
            )
            self.job_task_mgr.make_failure(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            raise ERROR_COLLECTOR_SECRET(plugin_info=plugin_info, param=secret_id)

        try:
            # Update JobTask (In-progress)
            self._update_job_task(job_task_id, 'IN_PROGRESS', domain_id, secret=self.secret)
        except Exception as e:
            _LOGGER.error(f'[collecting_resources] fail to update job_task: {e}')

        ##########################################################
        # Call method
        ##########################################################
        try:
            _LOGGER.debug('[collect] Before call collect')
            results = connector.collect(plugin_info['options'], secret_data.get('data', {}), collect_filter)
            _LOGGER.debug('[collect] generator: %s' % results)

        except ERROR_BASE as e:
            _LOGGER.error(f'[collecting_resources] fail to collect: {e.message}')
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                e.error_code,
                e.message
            )
            self.job_task_mgr.make_failure(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            raise ERROR_COLLECTOR_COLLECTING(plugin_info=plugin_info, filters=collect_filter)

        except Exception as e:
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                'ERROR_COLLECTOR_COLLECTING',
                e
            )
            self.job_task_mgr.make_failure(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            raise ERROR_COLLECTOR_COLLECTING(plugin_info=plugin_info, filters=collect_filter)

        ##############################################################
        # Processing Result
        # Type 1: use_db_queue == False, processing synchronously
        # Type 2: use_db_queue == True, processing asynchronously
        ##############################################################
        JOB_TASK_STATE = 'SUCCESS'
        stat = {}
        ERROR = False
        plugin_id = plugin_info.get('plugin_id', None)
        try:
            stat = self._process_results(results, job_id, job_task_id, collector_id, secret_id, plugin_id, domain_id)
            if stat['failure_count'] > 0:
                JOB_TASK_STATE = 'FAILURE'

        except ERROR_BASE as e:
            _LOGGER.error(f'[collecting_resources] {e}', exc_info=True)
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                e.error_code,
                e.message
            )
            JOB_TASK_STATE = 'FAILURE'
            ERROR = True

        except Exception as e:
            _LOGGER.error(f'[collecting_resources] {e}', exc_info=True)
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                'ERROR_COLLECTOR_COLLECTING',
                e
            )
            JOB_TASK_STATE = 'FAILURE'
            ERROR = True

        finally:
            # update collection_state which is not found
            cleanup_mode = self._need_update_collection_state(plugin_info, filters)
            _LOGGER.debug(f'[collecting_resources] #### cleanup support: {cleanup_mode}')
            if cleanup_mode and JOB_TASK_STATE == 'SUCCESS':
                disconnected_count, deleted_count = self._update_collection_state(collector_id, secret_id,
                                                                                  job_task_id, domain_id)
                _LOGGER.debug(f'[collecting_resources] {job_task_id} => disconnected: {disconnected_count},'
                              f' total deleted: {deleted_count}')
                stat['disconnected_count'] = disconnected_count
                stat['deleted_count'] = deleted_count
            else:
                _LOGGER.debug(f'[collecting_resources] skip garbage_collection, {cleanup_mode}, {JOB_TASK_STATE}')

            if self.use_db_queue and ERROR is False:
                # WatchDog will finalize the task
                # if ERROR occurred, there is no data to processing
                pass
            else:
                if self.use_db_queue:
                    # delete cache
                    self._delete_job_task_stat_cache(job_id, job_task_id, domain_id)
                # Update Statistics of JobTask
                self._update_job_task(job_task_id, JOB_TASK_STATE, domain_id, stat=stat)
                # Update Job
                self.job_mgr.decrease_remained_tasks(kwargs['job_id'], domain_id)

        print("[collecting_resources] system tracker")
        return True

    @staticmethod
    def _need_update_collection_state(plugin_info, filters):
        try:
            if filters != {}:
                return False
            metadata = plugin_info.get('metadata', {})
            supported_features = metadata.get('supported_features', [])
            if 'garbage_collection' in supported_features:
                return True
            return False
        except Exception as e:
            _LOGGER.error(e)
            return False

    def _update_collection_state(self, collector_id, secret_id, job_task_id, domain_id):
        """ get cleanup manager
        cleanup_mgr = self.locator
        """
        try:
            cleanup_mgr = self.locator.get_manager('CleanupManager')
            return cleanup_mgr.update_collection_state(collector_id, secret_id, job_task_id, domain_id)
        except Exception as e:
            _LOGGER.error(f'[_update_collection_state] failed: {e}')
            return 0, 0

    def _process_results(self, results, job_id, job_task_id, collector_id, secret_id, plugin_id, domain_id):
        # update meta
        self.transaction.set_meta('job_id', job_id)
        self.transaction.set_meta('job_task_id', job_task_id)
        self.transaction.set_meta('collector_id', collector_id)
        self.transaction.set_meta('secret.secret_id', secret_id)
        self.transaction.set_meta('disable_info_log', 'true')
        if plugin_id:
            self.transaction.set_meta('plugin_id', plugin_id)
        if 'provider' in self.secret:
            self.transaction.set_meta('secret.provider', self.secret['provider'])
        if 'project_id' in self.secret:
            self.transaction.set_meta('secret.project_id', self.secret['project_id'])
        if 'service_account_id' in self.secret:
            self.transaction.set_meta('secret.service_account_id', self.secret['service_account_id'])

        created = 0
        updated = 0
        failure = 0

        idx = 0
        params = {
            'domain_id': domain_id,
            'job_id': job_id,
            'job_task_id': job_task_id,
            'collector_id': collector_id,
            'secret_id': secret_id
        }
        _LOGGER.debug(f'[_process_results] processing results')
        if self.use_db_queue:
            self._create_job_task_stat_cache(job_id, job_task_id, domain_id)

        for res in results:
            try:
                res_dict = MessageToDict(res, preserving_proto_field_name=True)
                idx += 1
                _LOGGER.debug(f'[_process_results] idx: {idx}')
                ######################################
                # Asynchronous DB Updater (using Queue)
                ######################################
                if self.use_db_queue:
                    _LOGGER.debug(f'[_process_results] use db queue: {idx}')
                    # Create Asynchronous Task
                    pushed = self._create_db_update_task(res_dict, params)
                    if pushed is False:
                        failure += 1
                    continue

                #####################################
                # Synchronous Update
                # If you here, processing in worker
                #####################################
                res_state = self._process_single_result(res_dict, params)

                if res_state == NOT_COUNT:
                    pass
                elif res_state == CREATED:
                    created += 1
                elif res_state == UPDATED:
                    updated += 1
                else:
                    # FAILURE
                    failure += 1

            except Exception as e:
                _LOGGER.error(f'[_process_results] failed single result {e}')
                failure += 1

        # Add watchdog for stat finalizing
        if self.use_db_queue:
            _LOGGER.debug(f'[_process_results] push watchdog, {job_task_id}')
            pushed = self._create_db_update_task_watchdog(idx, job_id, job_task_id, domain_id)

        _LOGGER.debug(f'[_process_results] number of idx: {idx}')
        # Update JobTask
        return {
            'total_count': idx,
            'created_count': created,
            'updated_count': updated,
            'failure_count': failure
        }

    def _process_single_result(self, resource, params):
        """ Process single resource (Add/Update)
            Args:
                resource (message_dict): resource from collector
                params (dict): {
                    'domain_id': 'str',
                    'job_id': 'str',
                    'job_task_id': 'str',
                    'collector_id': 'str',
                    'secret_id': 'str'
                }
            Returns:
                0: exclude at stat (for example, cloud_service_type)
                1: created
                2: updated
                3: error
        """
        # update meta
        domain_id = params['domain_id']
        resource_type = resource['resource_type']
        state = resource.get('state', 'None')
        data = resource.get('resource', {})

        options = resource.get('options', None)
        update_mode = None
        if options:
            # options exist,
            # {'update_mode': MERGE | REPLACE}
            if 'update_mode' in options:
                update_mode = options['update_mode']
                self.transaction.set_meta('update_mode', update_mode)
            # delete update_mode

        _LOGGER.debug(f'[_process_single_result] {resource_type}')
        (svc, mgr) = self._get_resource_map(resource_type)

        # FilterCache
        if resource_type == 'inventory.FilterCache' or resource_type == 'FILTER_CACHE':
            return mgr.cache_filter(params['collector_id'], params['secret_id'], data)

        data['domain_id'] = domain_id
        # General Resource like Server, CloudService
        match_rules = resource.get('match_rules', {})
        ##################################
        # Match rules
        ##################################
        job_id = params['job_id']
        job_task_id = params['job_task_id']
        response = ERROR

        ##################################
        # Error Resource
        ##################################
        if resource_type == "inventory.ErrorResource" and state == "FAILURE":
            # add error
            message = resource.get('message', 'No message from plugin')
            _LOGGER.error(f'[_process_single_result] Error resource: {resource}')
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                "ERROR_PLUGIN",
                message,
                data
            )
            if self.use_db_queue:
                self._update_job_task_stat_to_cache(job_id, job_task_id, ERROR, domain_id)
            return ERROR

        if match_rules == {}:
            # There may be no match rule, collector error
            _LOGGER.error(f'[_process_single_result] may be bug, no match rule: {resource}')
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                "ERROR_MATCH_RULE",
                f"No match rule found: {resource}",
                {'resource_type': resource_type}
            )
            if self.use_db_queue:
                self._update_job_task_stat_to_cache(job_id, job_task_id, ERROR, domain_id)
            return ERROR

        start = time.time()
        try:
            res_info, total_count = self._query_with_match_rules(data,
                                                                 match_rules,
                                                                 domain_id,
                                                                 mgr
                                                                 )
            _LOGGER.debug(f'[_process_single_result] matched resources count = {total_count}')
        except ERROR_TOO_MANY_MATCH as e:
            _LOGGER.error(f'[_process_single_result] too many match')
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                e.error_code,
                e.message,
                {'resource_type': resource_type}
            )
            total_count = ERROR
        except Exception as e:
            _LOGGER.error(f'[_process_single_result] failed to match: {e}')
            _LOGGER.warning(f'[_process_single_result] assume new resource, create')
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                "ERROR_UNKNOWN",
                "Match Query failed, may be DB problem",
                {'resource_type': resource_type}
            )
            total_count = ERROR

        end = time.time()
        diff = end - start
        _LOGGER.debug(f'query time: {diff}')

        #########################################
        # Create / Update to DB
        #########################################
        try:
            # For book-keeping
            if total_count == 0 and update_mode is None:
                # Create Resource
                _LOGGER.debug(f'[_process_single_result] Create Resource: {resource_type}')
                svc.create(data)
                diff = time.time() - end
                _LOGGER.debug(f'insert: {diff}')
                response = CREATED

            elif total_count == 1:
                # Update Resource
                _LOGGER.debug(f'[_process_single_result] Update Resource: {resource_type}')
                data.update(res_info[0])
                svc.update(data)
                diff = time.time() - end
                _LOGGER.debug(f'update: {diff}')
                response = UPDATED

            elif total_count > 1:
                # Ambiguous
                _LOGGER.error(f'[_process_single_result] will not reach here!')
                # This is raise at _query_with_match_rules
                response = ERROR

            if self.use_db_queue:
                self._update_job_task_stat_to_cache(job_id, job_task_id, response, domain_id)

        except ERROR_BASE as e:
            _LOGGER.error(f'[_process_single_result] ERROR: {data}')
            # May be DB error
            additional = {'resource_type': resource_type}

            if resource_type == 'inventory.CloudService':
                additional['cloud_service_group'] = data.get('cloud_service_group')
                additional['cloud_service_type'] = data.get('cloud_service_type')
                additional['provider'] = data.get('provider')

            if total_count == 1:
                if resource_type == 'inventory.Server':
                    additional['resource_id'] = data.get('server_id')
                elif resource_type == 'inventory.CloudService':
                    additional['resource_id'] = data.get('cloud_service_id')
                elif resource_type == 'inventory.CloudServiceType':
                    additional['resource_id'] = data.get('cloud_service_type_id')
                elif resource_type == 'inventory.Region':
                    additional['resource_id'] = data.get('region_id')

            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                e.error_code,
                e.message,
                additional
            )
            response = ERROR

        except Exception as e:
            # TODO: create error message
            _LOGGER.debug(f'[_process_single_result] service error: {svc}, {e}')
            response = ERROR
        finally:
            if response in [CREATED, UPDATED]:
                if resource_type in ['inventory.CloudServiceType', 'inventory.Region']:
                    response = NOT_COUNT
            return response

    def _get_resource_map(self, resource_type):
        """ Base on resource type
        Returns: (service, manager)
        """
        if resource_type not in RESOURCE_MAP:
            raise ERROR_UNSUPPORTED_RESOURCE_TYPE(resource_type=resource_type)
        if resource_type not in SERVICE_MAP:
            raise ERROR_UNSUPPORTED_RESOURCE_TYPE(resource_type=resource_type)

        # Get proper manager
        # Create new manager or service, since transaction is variable
        svc = self.locator.get_service(SERVICE_MAP[resource_type], metadata=self.transaction.meta)
        mgr = self.locator.get_manager(RESOURCE_MAP[resource_type])
        return (svc, mgr)

    def _update_job_task(self, job_task_id, state, domain_id, secret=None, stat=None):
        """ Update JobTask
        - state (Pending -> In-progress)
        - started_time
        - secret_info
        """

        # Update Secret also
        secret_info = None
        if secret:
            secret_info = {
                'secret_id': secret['secret_id']
            }
            provider = secret.get('provider')
            service_account_id = secret.get('service_account_id')
            project_id = secret.get('project_id')
            if provider:
                secret_info.update({'provider': provider})
            if service_account_id:
                secret_info.update({'service_account_id': service_account_id})
            if project_id:
                secret_info.update({'project_id': project_id})
            _LOGGER.debug(f'[_update_job_task] secret_info: {secret_info}')

        if state == 'IN_PROGRESS':
            self.job_task_mgr.make_inprogress(job_task_id, domain_id, secret_info, stat)
        elif state == 'SUCCESS':
            self.job_task_mgr.make_success(job_task_id, domain_id, secret_info, stat)
        elif state == 'FAILURE':
            self.job_task_mgr.make_failure(job_task_id, domain_id, secret_info, stat)
        elif state == 'CANCELED':
            self.job_task_mgr.make_canceled(job_task_id, domain_id, secret_info, stat)
        else:
            _LOGGER.error(f'[_update_job_task] undefined state: {state}')
            self.job_task_mgr.make_failure(job_task_id, domain_id, secret_info, stat)

    @staticmethod
    def _create_job_task_stat_cache(job_id, job_task_id, domain_id):
        """ Update to cache
        Args:
            - kind: CREATED | UPDATED | ERROR
        cache key
            - job_task_stat:<job_id>:<job_task_id>:created = N
            - job_task_stat:<job_id>:<job_task_id>:updated = M
            - job_task_stat:<job_id>:<job_task_id<:failure = X
        """
        try:
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:CREATED'
            cache.set(key, 0, expire=JOB_TASK_STAT_EXPIRE_TIME)
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:UPDATED'
            cache.set(key, 0, expire=JOB_TASK_STAT_EXPIRE_TIME)
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:FAILURE'
            cache.set(key, 0, expire=JOB_TASK_STAT_EXPIRE_TIME)
        except Exception as e:
            _LOGGER.error(f'[_create_job_task_stat_cache] {e}')

    @staticmethod
    def _delete_job_task_stat_cache(job_id, job_task_id, domain_id):
        """ Delete cache
        Args:
            - kind: CREATED | UPDATED | ERROR
        cache key
            - job_task_stat:<job_id>:<job_task_id>:created = N
            - job_task_stat:<job_id>:<job_task_id>:updated = M
            - job_task_stat:<job_id>:<job_task_id<:failure = X
        """
        try:
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:CREATED'
            cache.delete(key)
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:UPDATED'
            cache.delete(key)
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:FAILURE'
            cache.delete(key)
        except Exception as e:
            _LOGGER.error(f'[_delete_job_task_stat_cache] {e}')

    @staticmethod
    def _update_job_task_stat_to_cache(job_id, job_task_id, kind, domain_id):
        """ Update to cache
        Args:
            - kind: CREATED | UPDATED | ERROR
        cache key
            - job_task_stat:<job_id>:<job_task_id>:created = N
            - job_task_stat:<job_id>:<job_task_id>:updated = M
            - job_task_stat:<job_id>:<job_task_id<:failure = X
        """
        if kind == CREATED:
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:CREATED'
        elif kind == UPDATED:
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:UPDATED'
        # elif kind == ERROR:
        else:
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:FAILURE'

        cache.increment(key)

    def _watchdog_job_task_stat(self, param):
        """ WatchDog for cache stat
        1) Update to DB
        2) Update JobTask status
        param = {
            'job_id': job_id,
            'job_task_id': job_task_id,
            'domain_id': domain_id,
            'total_count': total_count
            }
        """
        # Wait a little, may be working task exist
        _LOGGER.debug(f'[_watchdog_job_task_stat] WatchDog Start: {param["job_task_id"]}')
        time.sleep(WATCHDOG_WAITING_TIME)
        domain_id = param['domain_id']
        job_id = param['job_id']
        job_task_id = param['job_task_id']

        try:
            key_created = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:CREATED'
            value_created = cache.get(key_created)
            cache.delete(key_created)
        except Exception:
            value_created = 0

        try:
            key_updated = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:UPDATED'
            value_updated = cache.get(key_updated)
            cache.delete(key_updated)
        except Exception:
            value_updated = 0

        try:
            key_failure = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:FAILURE'
            value_failure = cache.get(key_failure)
            cache.delete(key_failure)
        except Exception:
            value_failure = 0

        # Update to DB
        stat_result = {
            'total_count': param['total_count'],
            'created_count': value_created,
            'updated_count': value_updated,
            'failure_count': value_failure
        }

        _LOGGER.debug(f'[_watchdog_job_task_stat] stat: {stat_result}')
        try:
            if stat_result['failure_count'] > 0:
                JOB_TASK_STATE = 'FAILURE'
            else:
                JOB_TASK_STATE = 'SUCCESS'
            self._update_job_task(job_task_id, JOB_TASK_STATE, domain_id, stat=stat_result)
        except Exception as e:
            # error
            pass
        finally:
            # Close remained task
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)

    ######################
    # Internal
    ######################
    def _get_connector(self, plugin_info, domain_id, **kwargs):
        """ Find proper connector(plugin)

        Returns: connector (object)
        """
        connector = self.locator.get_connector('CollectorPluginConnector')
        # get endpoint
        endpoint, updated_version = self._get_endpoint(plugin_info, domain_id)
        _LOGGER.debug('[collect] endpoint: %s' % endpoint)
        connector.initialize(endpoint)

        return connector

    def _get_endpoint(self, plugin_info, domain_id):
        """ get endpoint from plugin_info

        Args:
            plugin_info (dict) : {
                'plugin_id': 'str',
                'version': 'str',
                'options': 'dict',
                'secret_id': 'str',
                'secret_group_id': 'str',
                'provider': 'str',
                'capabilities': 'dict'
                }
            domain_id (str)

        Returns: Endpoint Object

        """
        # Call Plugin Service
        plugin_id = plugin_info['plugin_id']
        version = plugin_info['version']
        upgrade_mode = plugin_info.get('upgrade_mode', 'AUTO')

        plugin_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', service='plugin')

        response = plugin_connector.dispatch('Plugin.get_plugin_endpoint', {
            'plugin_id': plugin_id,
            'version': version,
            'upgrade_mode': upgrade_mode,
            'domain_id': domain_id
        })

        return response.get('endpoint'), response.get('updated_version')

    @staticmethod
    def _query_with_match_rules(resource, match_rules, domain_id, mgr):
        """ match resource based on match_rules

        Args:
            resource: ResourceInfo(Json) from collector plugin
            match_rules:
                ex) {1:['data.vm.vm_id'], 2:['zone_id', 'data.ip_addresses']}

        Return:
            resource_id : resource_id for resource update (ex. {'server_id': 'server-xxxxxx'})
            True: can not determine resources (ambiguous)
            False: no matched
        """

        found_resource = None
        total_count = 0

        match_rules = rule_matcher.dict_key_int_parser(match_rules)

        match_order = match_rules.keys()

        for order in sorted(match_order):
            query = rule_matcher.make_query(order, match_rules, resource, domain_id)
            _LOGGER.debug(f'[_query_with_match_rules] query generated: {query}')
            found_resource, total_count = mgr.find_resources(query)
            if found_resource and total_count == 1:
                return found_resource, total_count
            if total_count > 0:
                # Raise Error, for detailed tracking
                if 'data' in resource:
                    data = resource['data']
                else:
                    data = resource
                raise ERROR_TOO_MANY_MATCH(match_key=match_rules[order], resources=found_resource, more=data)
        # total_count == 0
        return found_resource, total_count

    ########################
    # Asynchronous DB Update
    ########################
    def _create_db_update_task(self, res, param):
        """ Create Asynchronous Task
        """
        try:
            # Push Queue
            task = {'method': '_process_single_result', 'res': res, 'param': param, 'meta': self.transaction.meta}
            json_task = json.dumps(task)
            queue.put(self.db_queue, json_task)
            return True
        except Exception as e:
            _LOGGER.error(f'[_create_db_update_task] {e}')
            return False

    def _create_db_update_task_watchdog(self, total_count, job_id, job_task_id, domain_id):
        """ Create Asynchronous Task
        """
        try:
            # Push Queue
            param = {'job_id': job_id, 'job_task_id': job_task_id, 'domain_id': domain_id, 'total_count': total_count}
            task = {'method': '_watchdog_job_task_stat', 'res': {}, 'param': param, 'meta': self.transaction.meta}
            json_task = json.dumps(task)
            queue.put(self.db_queue, json_task)
            return True
        except Exception as e:
            _LOGGER.error(f'[_create_db_update_task_watchdog] {e}')
            return False
