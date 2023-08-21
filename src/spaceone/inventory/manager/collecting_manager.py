import logging
import json
import time
from spaceone.core import config, cache
from spaceone.core import queue
# from spaceone.core.transaction import LOCAL_STORAGE
from spaceone.core.manager import BaseManager
from spaceone.inventory.manager.job_manager import JobManager
from spaceone.inventory.manager.job_task_manager import JobTaskManager
from spaceone.inventory.manager.plugin_manager import PluginManager
from spaceone.inventory.manager.cleanup_manager import CleanupManager
from spaceone.inventory.manager.collector_plugin_manager import CollectorPluginManager
from spaceone.inventory.error import *
from spaceone.inventory.lib import rule_matcher
from spaceone.inventory.conf.collector_conf import *

_LOGGER = logging.getLogger(__name__)


class CollectingManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_mgr: JobManager = self.locator.get_manager(JobManager)
        self.job_task_mgr: JobTaskManager = self.locator.get_manager(JobTaskManager)
        self.db_queue = DB_QUEUE_NAME

    def collecting_resources(self, params):
        """ Execute collecting task to get resources from plugin
        Args:
            params(dict): {
                collector_id(str)
                job_id(str)
                job_task_id(str)
                domain_id(str)
                plugin_info(dict)
                task_options(dict)
                secret_info(dict)
                secret_data(dict)
            }
        """
        plugin_manager: PluginManager = self.locator.get_manager(PluginManager)
        collector_plugin_mgr: CollectorPluginManager = self.locator.get_manager(CollectorPluginManager)

        collector_id = params['collector_id']
        job_id = params['job_id']
        job_task_id = params['job_task_id']
        domain_id = params['domain_id']
        task_options = params['task_options']

        secret_info = params['secret_info']
        secret_id = secret_info['secret_id']
        secret_data = params['secret_data']
        plugin_info = params['plugin_info']

        _LOGGER.debug(f'[collecting_resources] Job Task ID: {job_task_id}')

        if self.job_mgr.check_cancel(job_id, domain_id):
            self.job_task_mgr.add_error(job_task_id, domain_id, 'ERROR_COLLECT_CANCELED', 'The job has been canceled.')
            self.job_task_mgr.make_failure(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            self.job_mgr.increase_failure_tasks(job_id, domain_id)
            raise ERROR_COLLECT_CANCELED(job_id=job_id)

        collect_filter = {}

        try:
            # JOB TASK: IN_PROGRESS
            self._update_job_task(job_task_id, 'IN_PROGRESS', domain_id, secret_info=secret_info)
        except Exception as e:
            _LOGGER.error(f'[collecting_resources] fail to update job_task: {e}')

        try:
            # EXECUTE PLUGIN COLLECTION
            endpoint, updated_version = plugin_manager.get_endpoint(plugin_info['plugin_id'], plugin_info.get('version'), domain_id, plugin_info.get('upgrade_mode', 'AUTO'))
            results = collector_plugin_mgr.collect(endpoint, plugin_info['options'], secret_data.get('data', {}), collect_filter, task_options)     # task_options = None
            # DELETE secret_data in params for Secure
            del params['secret_data']
        except Exception as e:
            _LOGGER.error(f'[collecting_resources - ERROR {job_id}/{job_task_id}] {e}', exc_info=True)
            self.job_task_mgr.add_error(job_task_id, domain_id, 'ERROR_COLLECTOR_COLLECTING', e)
            self.job_task_mgr.make_failure(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            self.job_mgr.increase_failure_tasks(job_id, domain_id)
            raise ERROR_COLLECTOR_COLLECTING(plugin_info=plugin_info, filters=collect_filter)

        JOB_TASK_STATE = 'SUCCESS'
        collecting_count_info = {}

        try:
            collecting_count_info = self._check_collecting_results(results, params)

            if collecting_count_info['failure_count'] > 0:
                JOB_TASK_STATE = 'FAILURE'

        except Exception as e:
            _LOGGER.error(f'[collecting_resources] {e}', exc_info=True)
            self.job_task_mgr.add_error(job_task_id, domain_id, 'ERROR_COLLECTOR_COLLECTING', e)
            self.job_task_mgr.make_failure(job_task_id, domain_id)
            JOB_TASK_STATE = 'FAILURE'

        finally:
            cleanup_mode = self._check_garbage_collection_mode(plugin_info)

            if cleanup_mode and JOB_TASK_STATE == 'SUCCESS':
                disconnected_count, deleted_count = self._update_disconnected_and_deleted_count(collector_id, secret_id, job_task_id, domain_id)
                collecting_count_info.update({'disconnected_count': disconnected_count, 'deleted_count': deleted_count})
                _LOGGER.debug(f'[collecting_resources] {job_task_id} | disconnected: {disconnected_count}, deleted: {deleted_count}')
            else:
                _LOGGER.debug(f'[collecting_resources] skip garbage_collection, {cleanup_mode}, {JOB_TASK_STATE}')

            _LOGGER.debug(f'[collecting_resources] {job_task_id} | collecting_count_info: {collecting_count_info}')
            self._update_job_task(job_task_id, JOB_TASK_STATE, domain_id, secret_info=secret_info, collecting_count_info=collecting_count_info)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)

            if JOB_TASK_STATE == 'SUCCESS':
                self.job_mgr.increase_success_tasks(job_id, domain_id)
            elif JOB_TASK_STATE == 'FAILURE':
                self.job_mgr.increase_failure_tasks(job_id, domain_id)

            # debug code for memory leak
            # local_storage = LOCAL_STORAGE.__dict__
            # _LOGGER.info(
            #     f'[collecting_resources] / number of items in local storage: {len(local_storage)} / items => {local_storage}')
        return True

    def _update_disconnected_and_deleted_count(self, collector_id, secret_id, job_task_id, domain_id):
        try:
            cleanup_mgr: CleanupManager = self.locator.get_manager(CleanupManager)
            return cleanup_mgr.update_disconnected_and_deleted_count(collector_id, secret_id, job_task_id, domain_id)
        except Exception as e:
            _LOGGER.error(f'[_update_collection_state] failed: {e}')
            return 0, 0

    def _check_collecting_results(self, results, params):
        """
        Args:
            params(dict): {
                collector_id(str)
                job_id(str)
                job_task_id(str)
                domain_id(str)
                plugin_info(dict)
                task_options(dict)
                secret_info(dict)
            }
        """
        created_count = 0
        updated_count = 0
        failure_count = 0
        total_count = 0

        self._set_transaction_meta(params)

        for res in results:
            total_count += 1
            try:
                res_state = self.check_resource_state(res, params)

                if res_state == NOT_COUNT:
                    pass
                elif res_state == CREATED:
                    created_count += 1
                elif res_state == UPDATED:
                    updated_count += 1
                else:
                    failure_count += 1

            except Exception as e:
                _LOGGER.error(f'[_process_results] failed single result {e}')
                failure_count += 1

        return {
            'total_count': total_count,
            'created_count': created_count,
            'updated_count': updated_count,
            'failure_count': failure_count
        }

    def check_resource_state(self, resource, params):
        """
        Args:
            resource (message_dict): resource from collector
            params(dict): {
                collector_id(str)
                job_id(str)
                job_task_id(str)
                domain_id(str)
                plugin_info(dict)
                task_options(dict)
                secret_info(dict)
            }
        Returns:
            0: NOT_COUNT (for example, cloud_service_type)
            1: CREATED
            2: UPDATED
            3: ERROR
        """
        job_task_id = params['job_task_id']
        domain_id = params['domain_id']
        resource_type = resource.get('resource_type')
        state = resource.get('state', 'None')
        data = resource.get('resource', {})

        if update_mode := resource.get('options', {}).get('update_mode'):   # MERGE | REPLACE
            self.transaction.set_meta('update_mode', update_mode)

        resource_service, resource_manager = self._get_resource_map(resource_type)
        data['domain_id'] = domain_id
        response = ERROR

        if resource_type == "inventory.ErrorResource" and state == "FAILURE":
            err_message = resource.get('message', 'No message from plugin')
            _LOGGER.error(f'[_process_single_result] Error resource: {err_message}')
            self.job_task_mgr.add_error(job_task_id, domain_id, "ERROR_PLUGIN", err_message, data)
            return ERROR

        match_rules = resource.get('match_rules')

        if not match_rules:
            _msg = 'No match rule'
            _LOGGER.error(f'[_process_single_result] {_msg}')
            self.job_task_mgr.add_error(job_task_id, domain_id, "ERROR_MATCH_RULE", f"{_msg}: {resource}", {'resource_type': resource_type})
            return ERROR

        try:
            res_info, total_count = self._query_with_match_rules(data, match_rules, domain_id, resource_manager)
        except ERROR_TOO_MANY_MATCH as e:
            _LOGGER.error(f'[_process_single_result] Too many match')
            self.job_task_mgr.add_error(job_task_id, domain_id, e.error_code, e.message, {'resource_type': resource_type})
            return ERROR
        except Exception as e:
            _LOGGER.error(f'[_process_single_result] failed to match: {e}')
            self.job_task_mgr.add_error(job_task_id, domain_id, "ERROR_UNKNOWN", "Match Query failed, may be DB problem", {'resource_type': resource_type})
            return ERROR

        try:
            # CREATE
            if total_count == 0 and update_mode is None:
                resource_service.create_resource(data)
                response = CREATED
            # UPDATE
            elif total_count == 1:
                data.update(res_info[0])
                resource_service.update_resource(data)
                response = UPDATED
            elif total_count > 1:
                _LOGGER.error(f'[_process_single_result] will not reach here!')
                response = ERROR
        except ERROR_BASE as e:
            _LOGGER.error(f'[_process_single_result] ERROR: {data}')
            additional = self._set_error_addition_info(resource_type, total_count, data)
            self.job_task_mgr.add_error(job_task_id, domain_id, e.error_code, e.message, additional)
            response = ERROR
        except Exception as e:
            # TODO: create error message
            _LOGGER.debug(f'[_process_single_result] service error: {resource_service}, {e}')
            response = ERROR

        finally:
            if response in [CREATED, UPDATED]:
                if resource_type in ['inventory.CloudServiceType', 'inventory.Region']:
                    response = NOT_COUNT
            return response

    def _set_transaction_meta(self, params):
        secret_info = params['secret_info']

        self.transaction.set_meta('job_id', params['job_id'])
        self.transaction.set_meta('job_task_id', params['job_task_id'])
        self.transaction.set_meta('collector_id', params['collector_id'])
        self.transaction.set_meta('secret.secret_id', secret_info['secret_id'])
        self.transaction.set_meta('disable_info_log', 'true')

        if plugin_id := params['plugin_info'].get('plugin_id'):
            self.transaction.set_meta('plugin_id', plugin_id)
        if 'provider' in secret_info:
            self.transaction.set_meta('secret.provider', secret_info['provider'])

        if 'project_id' in secret_info:
            self.transaction.set_meta('secret.project_id', secret_info['project_id'])
        if 'service_account_id' in secret_info:
            self.transaction.set_meta('secret.service_account_id', secret_info['service_account_id'])

    def _get_resource_map(self, resource_type):
        if resource_type not in RESOURCE_MAP:
            raise ERROR_UNSUPPORTED_RESOURCE_TYPE(resource_type=resource_type)

        svc = self.locator.get_service(RESOURCE_MAP[resource_type][0])
        mgr = self.locator.get_manager(RESOURCE_MAP[resource_type][1])
        return svc, mgr

    def _update_job_task(self, job_task_id, state, domain_id, secret_info=None, collecting_count_info=None):
        state_map = {
            'IN_PROGRESS': self.job_task_mgr.make_inprogress,
            'SUCCESS': self.job_task_mgr.make_success,
            'FAILURE': self.job_task_mgr.make_failure,
            'CANCELED': self.job_task_mgr.make_canceled
        }

        if secret_info:
            secret_info = {
                'secret_id': secret_info['secret_id'],
                'provider': secret_info.get('provider'),
                'service_account_id': secret_info.get('service_account_id'),
                'project_id': secret_info.get('project_id')
            }

        if state in state_map:
            state_map[state](job_task_id, domain_id, secret_info, collecting_count_info)
        else:
            _LOGGER.error(f'[_update_job_task] undefined state: {state}')
            self.job_task_mgr.make_failure(job_task_id, domain_id, secret_info, collecting_count_info)

    @staticmethod
    def _set_error_addition_info(resource_type, total_count, data):
        additional = {'resource_type': resource_type}

        if resource_type == 'inventory.CloudService':
            additional.update({
                'cloud_service_group': data.get('cloud_service_group'),
                'cloud_service_type': data.get('cloud_service_type'),
                'provider': data.get('provider'),
            })

        if total_count == 1:
            if resource_type == 'inventory.CloudService':
                additional['resource_id'] = data.get('cloud_service_id')
            elif resource_type == 'inventory.CloudServiceType':
                additional['resource_id'] = data.get('cloud_service_type_id')
            elif resource_type == 'inventory.Region':
                additional['resource_id'] = data.get('region_id')

        return additional

    @staticmethod
    def _delete_job_task_stat_cache(job_id, job_task_id, domain_id):
        try:
            for state in ['CREATED', 'UPDATED', 'FAILURE']:
                cache.delete(f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:{state}')
        except Exception as e:
            _LOGGER.error(f'[_delete_job_task_stat_cache] {e}')

    @staticmethod
    def _collector_filter_from_cache(collect_filter, collector_id, secret_id):
        key = f'collector-filter:{collector_id}:{secret_id}'

        if value := cache.get(key):
            _LOGGER.debug(f'[collecting_resources] cache -> {key}: {value}')
            collect_filter.update(value)

        return collect_filter

    def watchdog_job_task_stat(self, param):
        _LOGGER.debug(f'[watchdog_job_task_stat] WatchDog Start: {param["job_task_id"]}')

        domain_id = param['domain_id']
        job_id = param['job_id']
        job_task_id = param['job_task_id']

        time.sleep(WATCHDOG_WAITING_TIME)

        stat_result = {
            'total_count': param['total_count'],
            'created_count': 0,
            'updated_count': 0,
            'failure_count': 0
        }

        try:
            key_created = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:CREATED'
            stat_result['created_count'] = cache.get(key_created)

            key_updated = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:UPDATED'
            stat_result['updated_count'] = cache.get(key_updated)

            key_failure = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:FAILURE'
            stat_result['failure_count'] = cache.get(key_failure)
        except Exception as e:
            _LOGGER.error(f'[watchdog_job_task_stat] Get Count from Cache: {e}')
            pass

        self._delete_job_task_stat_cache((job_id, job_task_id, domain_id))

        try:
            if stat_result['failure_count'] > 0:
                JOB_TASK_STATE = 'FAILURE'
            else:
                JOB_TASK_STATE = 'SUCCESS'
            self._update_job_task(job_task_id, JOB_TASK_STATE, domain_id, collecting_count_info=stat_result)
        except Exception as e:
            pass

        finally:
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)

    @staticmethod
    def _check_garbage_collection_mode(plugin_info):
        return True if 'garbage_collection' in plugin_info.get('metadata', {}).get('supported_features', []) else False

    @staticmethod
    def _query_with_match_rules(resource, match_rules, domain_id, resource_manager):
        """ match resource based on match_rules

        Args:
            resource: ResourceInfo(Json) from collector plugin
            match_rules:
                ex) {1:['data.vm.vm_id'], 2:['zone_id', 'data.ip_addresses']}

        Return:
            match_resource : resource_id for resource update (ex. {'server_id': 'server-xxxxxx'})
            total_count : total count of matched resources
        """
        match_resource = None
        total_count = 0

        match_rules = rule_matcher.dict_key_int_parser(match_rules)

        for order in sorted(match_rules.keys()):
            query = rule_matcher.make_query(order, match_rules, resource, domain_id)
            match_resource, total_count = resource_manager.find_resources(query)

            if total_count > 1:
                data = resource['data'] if 'data' in resource else resource
                raise ERROR_TOO_MANY_MATCH(match_key=match_rules[order], resources=match_resource, more=data)
            elif total_count == 1 and match_resource:
                return match_resource, total_count

        return match_resource, total_count

    """
    Deprecated
    """
    def _create_db_update_task_watchdog(self, total_count, job_id, job_task_id, domain_id):
        try:
            # PUSH QUEUE
            param = {'job_id': job_id, 'job_task_id': job_task_id, 'domain_id': domain_id, 'total_count': total_count}
            task = {'method': 'watchdog_job_task_stat', 'res': {}, 'param': param, 'meta': self.transaction.meta}
            json_task = json.dumps(task)
            queue.put(self.db_queue, json_task)
            return True
        except Exception as e:
            _LOGGER.error(f'[_create_db_update_task_watchdog] {e}')
            return False

    """
    Deprecated
    """
    def _create_db_update_task(self, res, param):
        try:
            _LOGGER.debug(f'[_create_db_update_task] Push Update Task Queue')
            task = {'method': 'check_resource_state', 'res': res, 'param': param, 'meta': self.transaction.meta}
            json_task = json.dumps(task)
            queue.put(self.db_queue, json_task)
            return True
        except Exception as e:
            _LOGGER.error(f'[_create_db_update_task] {e}')
            return False

    """
    Deprecated
    """
    @staticmethod
    def _update_job_task_stat_to_cache(job_id, job_task_id, kind, domain_id):
        if kind in [CREATED, UPDATED]:
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:{kind}'
        else:
            key = f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:FAILURE'

        cache.increment(key)

    """
    Deprecated
    """
    @staticmethod
    def _create_job_task_stat_cache(job_id, job_task_id, domain_id):
        try:
            for state in ['CREATED', 'UPDATED', 'FAILURE']:
                cache.set(f'job_task_stat:{domain_id}:{job_id}:{job_task_id}:{state}', 0, expire=JOB_TASK_STAT_EXPIRE_TIME)
        except Exception as e:
            _LOGGER.error(f'[_create_job_task_stat_cache] {e}')
