import logging
from typing import Generator, Tuple
from spaceone.core.manager import BaseManager
from spaceone.inventory.lib.resource_manager import ResourceManager
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

    def collecting_resources(self, params: dict):
        """Execute collecting task to get resources from plugin
        Args:
            params (dict): {
                'collector_id': 'str',
                'job_id': 'str',
                'job_task_id': 'str',
                'domain_id': 'str',
                'plugin_info': 'dict',
                'task_options': 'dict',
                'secret_info': 'dict',
                'secret_data': 'dict'
            }
        """

        plugin_manager: PluginManager = self.locator.get_manager(PluginManager)
        collector_plugin_mgr: CollectorPluginManager = self.locator.get_manager(
            CollectorPluginManager
        )

        collector_id = params["collector_id"]
        job_id = params["job_id"]
        job_task_id = params["job_task_id"]
        domain_id = params["domain_id"]
        task_options = params["task_options"]

        secret_info = params["secret_info"]
        secret_id = secret_info["secret_id"]
        secret_data = params["secret_data"]
        plugin_info = params["plugin_info"]

        # add workspace_id to params from secret_info
        params["workspace_id"] = secret_info["workspace_id"]

        _LOGGER.debug(f"[collecting_resources] start job task: {job_task_id}")

        if self.job_mgr.check_cancel(job_id, domain_id):
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                "ERROR_COLLECT_CANCELED",
                "The job has been canceled.",
            )
            self.job_task_mgr.make_failure(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            self.job_mgr.increase_failure_tasks(job_id, domain_id)
            raise ERROR_COLLECT_CANCELED(job_id=job_id)

        collect_filter = {}

        try:
            # JOB TASK: IN_PROGRESS
            self._update_job_task(job_task_id, "IN_PROGRESS", domain_id)
        except Exception as e:
            _LOGGER.error(
                f"[collecting_resources] db update error ({job_task_id}): {e}"
            )

        try:
            # EXECUTE PLUGIN COLLECTION
            endpoint, updated_version = plugin_manager.get_endpoint(
                plugin_info["plugin_id"],
                domain_id,
                plugin_info.get("upgrade_mode", "AUTO"),
                plugin_info.get("version"),
            )

            resources = collector_plugin_mgr.collect(
                endpoint,
                plugin_info["options"],
                secret_data.get("data", {}),
                task_options,
            )

            # delete secret_data in params for security
            del params["secret_data"]
        except Exception as e:
            if isinstance(e, ERROR_BASE):
                error_message = e.message
            else:
                error_message = str(e)

            _LOGGER.error(
                f"[collecting_resources] plugin collecting error ({job_task_id}): {error_message}",
                exc_info=True,
            )
            self.job_task_mgr.add_error(
                job_task_id, domain_id, "ERROR_COLLECTOR_COLLECTING", error_message
            )

            self.job_task_mgr.make_failure(job_task_id, domain_id)
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)
            self.job_mgr.increase_failure_tasks(job_id, domain_id)
            raise ERROR_COLLECTOR_COLLECTING(
                plugin_info=plugin_info, filters=collect_filter
            )

        job_task_state = "SUCCESS"
        collecting_count_info = {}

        try:
            collecting_count_info = self._upsert_collecting_resources(resources, params)

            if collecting_count_info["failure_count"] > 0:
                job_task_state = "FAILURE"

        except Exception as e:
            if isinstance(e, ERROR_BASE):
                error_message = e.message
            else:
                error_message = str(e)

            _LOGGER.error(
                f"[collecting_resources] upsert resources error ({job_task_id}): {error_message}",
                exc_info=True,
            )
            self.job_task_mgr.add_error(
                job_task_id, domain_id, "ERROR_COLLECTOR_COLLECTING", error_message
            )
            self.job_task_mgr.make_failure(job_task_id, domain_id)
            job_task_state = "FAILURE"

        finally:
            if job_task_state == "SUCCESS":
                (
                    disconnected_count,
                    deleted_count,
                ) = self._update_disconnected_and_deleted_count(
                    collector_id, secret_id, job_task_id, domain_id
                )
                collecting_count_info.update(
                    {
                        "disconnected_count": disconnected_count,
                        "deleted_count": deleted_count,
                    }
                )
                _LOGGER.debug(
                    f"[collecting_resources] success job task ({job_task_id}) "
                    f"(disconnected = {disconnected_count}, deleted = {deleted_count})"
                )

            _LOGGER.debug(
                f"[collecting_resources] job task summary ({job_task_id}) => {collecting_count_info}"
            )
            self._update_job_task(
                job_task_id,
                job_task_state,
                domain_id,
                collecting_count_info=collecting_count_info,
            )
            self.job_mgr.decrease_remained_tasks(job_id, domain_id)

            if job_task_state == "SUCCESS":
                self.job_mgr.increase_success_tasks(job_id, domain_id)
            elif job_task_state == "FAILURE":
                self.job_mgr.increase_failure_tasks(job_id, domain_id)

        return True

    def _update_disconnected_and_deleted_count(
        self, collector_id: str, secret_id: str, job_task_id: str, domain_id: str
    ) -> Tuple[int, int]:
        try:
            cleanup_mgr: CleanupManager = self.locator.get_manager(CleanupManager)
            return cleanup_mgr.update_disconnected_and_deleted_count(
                collector_id, secret_id, job_task_id, domain_id
            )
        except Exception as e:
            _LOGGER.error(f"[_update_collection_state] failed: {e}")
            return 0, 0

    def _upsert_collecting_resources(
        self, results: Generator[dict, None, None], params: dict
    ):
        """
        Args:
            params (Generator): {
                'collector_id': 'str',
                'job_id': 'str',
                'job_task_id': 'str',
                'workspace_id': 'str',
                'domain_id': 'str',
                'plugin_info': 'dict',
                'task_options': 'dict',
                'secret_info': 'dict'
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
                upsert_result = self._upsert_resource(res, params)

                if upsert_result == NOT_COUNT:
                    # skip count for cloud service type and region
                    pass
                elif upsert_result == CREATED:
                    created_count += 1
                elif upsert_result == UPDATED:
                    updated_count += 1
                else:
                    failure_count += 1

            except Exception as e:
                _LOGGER.error(
                    f"[_upsert_collecting_resources] upsert resource error: {e}"
                )
                failure_count += 1

        return {
            "total_count": total_count,
            "created_count": created_count,
            "updated_count": updated_count,
            "failure_count": failure_count,
        }

    def _upsert_resource(self, resource_data: dict, params: dict) -> int:
        """
        Args:
            resource_data (dict): resource information from plugin
            params(dict): {
                'collector_id': 'str',
                'job_id': 'str',
                'job_task_id': 'str',
                'workspace_id': 'str',
                'domain_id': 'str',
                'plugin_info': 'dict',
                'task_options': 'dict',
                'secret_info': 'dict'
            }
        Returns:
            0: NOT_COUNT (for cloud service type and region)
            1: CREATED
            2: UPDATED
            3: ERROR
        """

        job_task_id = params["job_task_id"]
        domain_id = params["domain_id"]
        workspace_id = params["workspace_id"]
        resource_type = resource_data.get("resource_type")
        resource_state = resource_data.get("state")
        match_rules = resource_data.get("match_rules")
        request_data = resource_data.get("resource", {})
        request_data["domain_id"] = domain_id
        request_data["workspace_id"] = workspace_id

        service, manager = self._get_resource_map(resource_type)

        response = ERROR

        if resource_state == "FAILURE":
            error_message = resource_data.get("message", "Unknown error.")
            _LOGGER.error(
                f"[_upsert_resource] plugin response error ({job_task_id}): {error_message}"
            )

            self.job_task_mgr.add_error(
                job_task_id, domain_id, "ERROR_PLUGIN", error_message, request_data
            )

            return ERROR

        if not match_rules:
            error_message = "Match rule is not defined."
            _LOGGER.error(
                f"[_upsert_resource] match rule error ({job_task_id}): {error_message}"
            )
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                "ERROR_MATCH_RULE",
                error_message,
                {"resource_type": resource_type},
            )
            return ERROR

        try:
            match_resource, total_count = self._query_with_match_rules(
                request_data, match_rules, domain_id, workspace_id, manager
            )

        except ERROR_TOO_MANY_MATCH as e:
            _LOGGER.error(
                f"[_upsert_resource] match resource error ({job_task_id}): {e}"
            )
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                e.error_code,
                e.message,
                {"resource_type": resource_type},
            )
            return ERROR
        except Exception as e:
            if isinstance(e, ERROR_BASE):
                error_message = e.message
            else:
                error_message = str(e)

            _LOGGER.error(
                f"[_upsert_resource] match resource error ({job_task_id}): {error_message}",
                exc_info=True,
            )
            self.job_task_mgr.add_error(
                job_task_id,
                domain_id,
                "ERROR_UNKNOWN",
                f"failed to match resource: {error_message}",
                {"resource_type": resource_type},
            )
            return ERROR

        try:
            if total_count == 0:
                # Create resource
                service.create_resource(request_data)
                response = CREATED
            elif total_count == 1:
                # Update resource
                request_data.update(match_resource[0])
                service.update_resource(request_data)
                response = UPDATED
            else:
                response = ERROR

        except ERROR_BASE as e:
            _LOGGER.error(
                f"[_upsert_resource] resource upsert error ({job_task_id}): {e.message}"
            )
            additional = self._set_error_addition_info(
                resource_type, total_count, request_data
            )
            self.job_task_mgr.add_error(
                job_task_id, domain_id, e.error_code, e.message, additional
            )
            response = ERROR

        except Exception as e:
            error_message = str(e)

            _LOGGER.debug(
                f"[_upsert_resource] unknown error ({job_task_id}): {error_message}",
                exc_info=True,
            )
            response = ERROR

        finally:
            if response in [CREATED, UPDATED]:
                if resource_type in ["inventory.CloudServiceType", "inventory.Region"]:
                    response = NOT_COUNT

            return response

    def _set_transaction_meta(self, params):
        secret_info = params["secret_info"]

        self.transaction.set_meta("job_id", params["job_id"])
        self.transaction.set_meta("job_task_id", params["job_task_id"])
        self.transaction.set_meta("collector_id", params["collector_id"])
        self.transaction.set_meta("secret.secret_id", secret_info["secret_id"])
        self.transaction.set_meta("disable_info_log", "true")

        if plugin_id := params["plugin_info"].get("plugin_id"):
            self.transaction.set_meta("plugin_id", plugin_id)

        if "provider" in secret_info:
            self.transaction.set_meta("secret.provider", secret_info["provider"])

        if "project_id" in secret_info:
            self.transaction.set_meta("secret.project_id", secret_info["project_id"])

        if "service_account_id" in secret_info:
            self.transaction.set_meta(
                "secret.service_account_id", secret_info["service_account_id"]
            )

    def _get_resource_map(self, resource_type: str):
        if resource_type not in RESOURCE_MAP:
            raise ERROR_UNSUPPORTED_RESOURCE_TYPE(resource_type=resource_type)

        service = self.locator.get_service(RESOURCE_MAP[resource_type][0])
        manager = self.locator.get_manager(RESOURCE_MAP[resource_type][1])
        return service, manager

    def _update_job_task(
        self,
        job_task_id: str,
        status: str,
        domain_id: str,
        collecting_count_info: dict = None,
    ) -> None:
        status_map = {
            "IN_PROGRESS": self.job_task_mgr.make_inprogress,
            "SUCCESS": self.job_task_mgr.make_success,
            "FAILURE": self.job_task_mgr.make_failure,
            "CANCELED": self.job_task_mgr.make_canceled,
        }

        if status in status_map:
            status_map[status](job_task_id, domain_id, collecting_count_info)
        else:
            _LOGGER.error(
                f"[_update_job_task] job task status is not defined: {status}"
            )
            self.job_task_mgr.make_failure(
                job_task_id, domain_id, collecting_count_info
            )

    @staticmethod
    def _set_error_addition_info(
        resource_type: str, total_count: int, resource_data: dict
    ) -> dict:
        additional = {"resource_type": resource_type}

        if resource_type == "inventory.CloudService":
            additional.update(
                {
                    "cloud_service_group": resource_data.get("cloud_service_group"),
                    "cloud_service_type": resource_data.get("cloud_service_type"),
                    "provider": resource_data.get("provider"),
                }
            )

        if total_count == 1:
            if resource_type == "inventory.CloudService":
                additional["resource_id"] = resource_data.get("cloud_service_id")
            elif resource_type == "inventory.CloudServiceType":
                additional["resource_id"] = resource_data.get("cloud_service_type_id")
            elif resource_type == "inventory.Region":
                additional["resource_id"] = resource_data.get("region_id")

        return additional

    @staticmethod
    def _query_with_match_rules(
        resource_data: dict,
        match_rules: dict,
        domain_id: str,
        workspace_id: str,
        resource_manager: ResourceManager,
    ):
        """match resource based on match rules

        Args:
            resource_data (dict): resource data from plugin
            match_rules (list): e.g. {1:['reference.resource_id'], 2:['name']}

        Return:
            match_resource (dict) : resource_id for update (e.g. {'cloud_service_id': 'cloud-svc-abcde12345'})
            total_count (int) : total count of matched resources
        """
        match_resource = None
        total_count = 0

        match_rules = rule_matcher.dict_key_int_parser(match_rules)

        for order in sorted(match_rules.keys()):
            query = rule_matcher.make_query(
                order, match_rules, resource_data, domain_id, workspace_id
            )
            match_resource, total_count = resource_manager.find_resources(query)

            if total_count > 1:
                if data := resource_data.get("data"):
                    raise ERROR_TOO_MANY_MATCH(
                        match_key=match_rules[order],
                        resources=match_resource,
                        more=data,
                    )
            elif total_count == 1 and match_resource:
                return match_resource, total_count

        return match_resource, total_count
