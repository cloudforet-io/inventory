import logging
import copy
from spaceone.core.service import *
from spaceone.inventory.error import *
from spaceone.inventory.manager.collection_state_manager import CollectionStateManager
from spaceone.inventory.manager.collector_manager import CollectorManager
from spaceone.inventory.manager.collector_plugin_manager import CollectorPluginManager
from spaceone.inventory.manager.collector_rule_manager import CollectorRuleManager
from spaceone.inventory.manager.repository_manager import RepositoryManager
from spaceone.inventory.manager.plugin_manager import PluginManager
from spaceone.inventory.manager.secret_manager import SecretManager
from spaceone.inventory.manager.job_manager import JobManager
from spaceone.inventory.manager.job_task_manager import JobTaskManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.model.collector_model import Collector

_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ["collector_id", "name", "provider"]


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CollectorService(BaseService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collector_mgr: CollectorManager = self.locator.get_manager(
            "CollectorManager"
        )

    @transaction(
        permission="inventory:Collector.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["name", "plugin_info", "plugin_info.plugin_id", "domain_id"])
    def create(self, params: dict) -> Collector:
        """Create collector
        Args:
            params (dict): {
                'name': 'str',              # required
                'plugin_info': 'dict',      # required
                'schedule': 'dict',
                'secret_filter': 'dict',
                'provider': 'str',
                'tags': 'dict',
                'resource_group': 'str',    # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth (required)
            }

        Returns:
            collector_vo (object)
        """

        identity_mgr: IdentityManager = self.locator.get_manager(IdentityManager)

        # Check permission by resource group
        if params["resource_group"] == "WORKSPACE":
            if "workspace_id" not in params:
                raise ERROR_REQUIRED_PARAMETER(key="workspace_id")

            identity_mgr.check_workspace(params["workspace_id"], params["domain_id"])
        else:
            params["workspace_id"] = "*"

        plugin_manager = self.locator.get_manager(PluginManager)
        collector_plugin_mgr: CollectorPluginManager = self.locator.get_manager(
            CollectorPluginManager
        )

        plugin_info = params["plugin_info"]
        plugin_id = plugin_info["plugin_id"]
        domain_id = params["domain_id"]

        plugin_info_from_repository = self._get_plugin_from_repository(plugin_id)
        capability = plugin_info_from_repository.get("capability", {})
        plugin_provider = self._get_plugin_providers(
            params.get("provider"), plugin_info_from_repository
        )

        params["capability"] = capability
        params["provider"] = plugin_provider

        if "secret_filter" in params:
            self.validate_secret_filter(
                identity_mgr, params["secret_filter"], domain_id
            )

        collector_vo = self.collector_mgr.create_collector(params)

        endpoint, updated_version = plugin_manager.get_endpoint(
            plugin_info["plugin_id"],
            domain_id,
            plugin_info.get("upgrade_mode", "AUTO"),
            plugin_info.get("version"),
        )

        plugin_response = collector_plugin_mgr.init_plugin(
            endpoint, plugin_info.get("options", {})
        )

        if updated_version:
            plugin_info["version"] = updated_version

        plugin_info["metadata"] = plugin_response.get("metadata", {})

        collector_vo = self.collector_mgr.update_collector_by_vo(
            collector_vo, {"plugin_info": plugin_info}
        )

        collector_rules = plugin_info["metadata"].get("collector_rules", [])
        self.create_collector_rules_by_metadata(
            collector_rules,
            collector_vo.collector_id,
            params["domain_id"],
            params["resource_group"],
            params.get("workspace_id"),
        )

        return collector_vo

    @transaction(
        permission="inventory:Collector.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_id", "domain_id"])
    def update(self, params: dict) -> Collector:
        """Update collector
        Args:
            params (dict): {
                'collector_id': 'str',      # required
                'name': 'str',
                'schedule': 'dict',
                'secret_filter': 'dict',
                'tags': 'dict',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth (required)
            }

        Returns:
            collector_vo (object)
        """

        collector_vo = self.collector_mgr.get_collector(
            params["collector_id"], params["domain_id"], params.get("workspace_id")
        )

        if "secret_filter" in params:
            self.validate_secret_filter(params["secret_filter"], params["domain_id"])

        return self.collector_mgr.update_collector_by_vo(collector_vo, params)

    @transaction(
        permission="inventory:Collector.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_id", "domain_id"])
    def delete(self, params: dict) -> None:
        """Delete collector
        Args:
            params (dict): {
                'collector_id': 'str',      # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth (required)
            }

        Returns:
            None:
        """

        state_mgr: CollectionStateManager = self.locator.get_manager(
            CollectionStateManager
        )

        collector_vo = self.collector_mgr.get_collector(
            params["collector_id"], params["domain_id"], params.get("workspace_id")
        )
        self.collector_mgr.delete_collector_by_vo(collector_vo)

        state_mgr.delete_collection_state_by_collector_id(
            params["collector_id"], params["domain_id"]
        )

    @transaction(
        permission="inventory:Collector.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["collector_id", "domain_id"])
    def get(self, params: dict) -> Collector:
        """Get collector
        Args:
            params (dict): {
                'collector_id': 'str',      # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth (required)
            }

        Returns:
            collector_vo (object)
        """

        collector_mgr: CollectorManager = self.locator.get_manager(CollectorManager)
        collector_id = params["collector_id"]
        domain_id = params["domain_id"]
        only = params.get("only")
        return collector_mgr.get_collector(collector_id, domain_id, only)

    @transaction(
        permission="inventory:Collector.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "collector_id",
            "name",
            "state",
            "priority",
            "plugin_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(_KEYWORD_FILTER)
    def list(self, params):
        """List collectors
        Args:
            params (dict): {
                    'query': 'dict (spaceone.api.core.v1.Query)',
                    'collector_id': 'str',
                    'name': 'str',
                    'secret_filter_state': 'str',
                    'schedule_state': 'str',
                    'plugin_id': 'str',
                    'workspace_id': 'str',          # injected from auth
                    'domain_id': 'str',             # injected from auth (required)
                }

        Returns:
            results (list)
            total_count (int)
        """

        collector_mgr: CollectorManager = self.locator.get_manager(CollectorManager)
        return collector_mgr.list_collectors(params.get("query", {}))

    @transaction(
        permission="inventory:Collector.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params):
        """Stat collectors
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)', # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        collector_mgr: CollectorManager = self.locator.get_manager(CollectorManager)
        return collector_mgr.stat_collectors(params.get("query", {}))

    @transaction(
        permission="inventory:Collector.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_id", "domain_id"])
    def collect(self, params):
        """Collect data
        Args:
            params (dict): {
                'collector_id': 'str',      # required
                'secret_id': 'str',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth (required)
            }

        Returns:
            job_vo (object)
        """

        plugin_mgr: PluginManager = self.locator.get_manager(PluginManager)
        job_mgr: JobManager = self.locator.get_manager(JobManager)
        job_task_mgr: JobTaskManager = self.locator.get_manager(JobTaskManager)

        collector_id = params["collector_id"]
        domain_id = params["domain_id"]
        collector_vo = self.collector_mgr.get_collector(collector_id, domain_id)
        collector_data = collector_vo.to_dict()

        plugin_info = collector_data["plugin_info"]
        secret_filter = collector_data.get("secret_filter", {})
        plugin_id = plugin_info["plugin_id"]
        version = plugin_info.get("version")
        upgrade_mode = plugin_info.get("upgrade_mode", "AUTO")

        endpoint, updated_version = plugin_mgr.get_endpoint(
            plugin_id, domain_id, upgrade_mode, version
        )

        if updated_version and version != updated_version:
            _LOGGER.debug(
                f"[collect] upgrade plugin version: {version} -> {updated_version}"
            )
            collector_vo = self._update_collector_plugin(
                endpoint, updated_version, plugin_info, collector_vo, domain_id
            )

        tasks = self.get_tasks(
            params,
            endpoint,
            collector_vo.provider,
            plugin_info,
            secret_filter,
            domain_id,
        )
        projects = self.list_projects_from_tasks(tasks)
        params.update(
            {
                "plugin_id": plugin_id,
                "total_tasks": len(tasks),
                "remained_tasks": len(tasks),
            }
        )

        duplicated_job_vos = job_mgr.list_duplicate_jobs(
            collector_id, params.get("secret_id"), domain_id
        )
        for job_vo in duplicated_job_vos:
            job_mgr.make_canceled_by_vo(job_vo)

        # JOB: IN-PROGRESS
        job_vo = job_mgr.create_job(collector_vo, params)

        if tasks:
            for task in tasks:
                task_options = task["task_options"]
                task.update({"collector_id": collector_id, "job_id": job_vo.job_id})

                try:
                    # JOB: CREATE TASK JOB
                    job_task_vo = job_task_mgr.create_job_task(
                        job_vo, domain_id, task_options
                    )
                    task.update({"job_task_id": job_task_vo.job_task_id})
                    job_task_mgr.push_job_task(task)

                except Exception as e:
                    job_mgr.add_error(
                        job_vo.job_id,
                        domain_id,
                        "ERROR_COLLECTOR_COLLECTING",
                        e,
                        {"task_options": task_options},
                    )
                    _LOGGER.error(
                        f"[collect] collecting failed: task_options={task_options}: {e}"
                    )
        else:
            # JOB: SUCCESS (No tasks)
            job_mgr.make_success_by_vo(job_vo)
            return job_vo

        self.collector_mgr.update_last_collected_time(collector_vo)
        return job_mgr.update_job_by_vo({"projects": projects}, job_vo)

    @transaction(
        permission="inventory:Collector.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_id", "domain_id"])
    def update_plugin(self, params):
        """Update plugin info of collector
        Args:
            params (dict): {
                'collector_id': 'str',      # required
                'version': 'str',
                'options': 'dict',
                'upgrade_mode': 'str',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth (required)
            }

        Returns:
            collector_vo (object)
        """

        plugin_manager: PluginManager = self.locator.get_manager(PluginManager)

        collector_id = params["collector_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        collector_vo = self.collector_mgr.get_collector(
            collector_id, domain_id, workspace_id
        )
        plugin_info = collector_vo.plugin_info.to_dict()

        if version := params.get("version"):
            plugin_info["version"] = version

        if options := params.get("options"):
            plugin_info["options"] = options

        if upgrade_mode := params.get("upgrade_mode"):
            plugin_info["upgrade_mode"] = upgrade_mode

        endpoint, updated_version = plugin_manager.get_endpoint(
            plugin_info["plugin_id"],
            plugin_info.get("version"),
            domain_id,
            plugin_info.get("upgrade_mode", "AUTO"),
        )

        collector_vo = self._update_collector_plugin(
            endpoint, updated_version, plugin_info, collector_vo, domain_id
        )
        return collector_vo

    @transaction(
        permission="inventory:Collector.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_id", "domain_id"])
    def verify_plugin(self, params):
        """Verify plugin info of collector
        Args:
            params (dict): {
                'collector_id': 'str',      # required
                'secret_id': 'str',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str'          # injected from auth (required)
            }

        Returns:
            collector_vo (object)
        """

        collector_plugin_mgr: CollectorPluginManager = self.locator.get_manager(
            CollectorPluginManager
        )
        plugin_manager: PluginManager = self.locator.get_manager(PluginManager)
        secret_manager: SecretManager = self.locator.get_manager(SecretManager)

        collector_id = params["collector_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        collector_vo = self.collector_mgr.get_collector(
            collector_id, domain_id, workspace_id
        )
        plugin_info = collector_vo.plugin_info.to_dict()

        endpoint, updated_version = plugin_manager.get_endpoint(
            plugin_info["plugin_id"],
            plugin_info.get("version"),
            domain_id,
            plugin_info.get("upgrade_mode", "AUTO"),
        )

        secret_ids = self.list_secret_from_secret_filter(
            plugin_info.get("secret_filter", {}),
            params.get("secret_id"),
            collector_vo.provider,
            domain_id,
        )

        if secret_ids:
            secret_data_info = secret_manager.get_secret_data(secret_ids[0], domain_id)
            secret_data = secret_data_info.get("data", {})
            collector_plugin_mgr.verify_plugin(
                endpoint, plugin_info.get("options", {}), secret_data
            )

    def get_tasks(
        self,
        params: dict,
        endpoint: str,
        collector_provider: str,
        plugin_info: dict,
        secret_filter: dict,
        domain_id: str,
    ):
        secret_mgr: SecretManager = self.locator.get_manager(SecretManager)
        collector_plugin_mgr: CollectorPluginManager = self.locator.get_manager(
            CollectorPluginManager
        )

        tasks = []
        secret_ids = self.list_secret_from_secret_filter(
            secret_filter, params.get("secret_id"), collector_provider, domain_id
        )

        for secret_id in secret_ids:
            secret_info = secret_mgr.get_secret(secret_id, domain_id)
            secret_data = secret_mgr.get_secret_data(secret_id, domain_id)

            _task = {
                "plugin_info": plugin_info,
                "secret_info": secret_info,
                "secret_data": secret_data,
                "domain_id": domain_id,
            }

            try:
                response = collector_plugin_mgr.get_tasks(
                    endpoint, secret_data, plugin_info.get("options", {})
                )
                _LOGGER.debug(f"[get_tasks] response: {response}")

                for task_options in response.get("tasks", []):
                    _task_dict = copy.deepcopy(_task)
                    _task_dict.update(task_options)
                    tasks.append(_task_dict)

            except Exception as e:
                # _LOGGER.debug(f'[get_tasks] Error to get tasks from plugin. set task from secret')
                _task.update({"task_options": None})
                tasks.append(_task)

        return tasks

    def validate_secret_filter(
        self, identity_mgr: IdentityManager, secret_filter: dict, domain_id: str
    ) -> None:
        if "secrets" in secret_filter:
            _query = {
                "filter": [
                    {"k": "secret_id", "v": secret_filter["secrets"], "o": "in"}
                ],
                "count_only": True,
            }
            secret_mgr: SecretManager = self.locator.get_manager(SecretManager)
            response = secret_mgr.list_secrets(_query, domain_id)
            total_count = response.get("total_count", 0)

            if total_count != len(secret_filter["secrets"]):
                raise ERROR_INVALID_PARAMETER(
                    key="secret_filter.secrets", reason="secrets not found."
                )

        if "service_accounts" in secret_filter:
            _query = {
                "filter": [
                    {
                        "k": "service_account_id",
                        "v": secret_filter["service_accounts"],
                        "o": "in",
                    }
                ],
                "count_only": True,
            }

            response = identity_mgr.list_service_accounts(_query)
            total_count = response.get("total_count", 0)

            if total_count != len(secret_filter["service_accounts"]):
                raise ERROR_INVALID_PARAMETER(
                    key="secret_filter.service_accounts",
                    reason="service accounts not found.",
                )

        if "schemas" in secret_filter:
            _query = {
                "filter": [
                    {"k": "name", "v": secret_filter["schemas"], "o": "in"},
                    {"k": "schema_type", "v": ["SECRET", "TRUSTING_SECRET"], "o": "in"},
                ],
                "count_only": True,
            }

            response = identity_mgr.list_schemas(_query)
            total_count = response.get("total_count", 0)

            if total_count != len(secret_filter["schemas"]):
                raise ERROR_INVALID_PARAMETER(
                    key="secret_filter.schema", reason="schema not found."
                )

    def _update_collector_plugin(
        self,
        endpoint: str,
        updated_version: str,
        plugin_info: dict,
        collector_vo: Collector,
        domain_id: str,
    ) -> Collector:
        collector_plugin_mgr: CollectorPluginManager = self.locator.get_manager(
            CollectorPluginManager
        )
        plugin_response = collector_plugin_mgr.init_plugin(
            endpoint, plugin_info.get("options", {})
        )

        if updated_version:
            plugin_info["version"] = updated_version

        plugin_info["metadata"] = plugin_response.get("metadata", {})

        params = {"plugin_info": plugin_info}
        collector_vo = self.collector_mgr.update_collector_by_vo(collector_vo, params)

        self.delete_collector_rules(collector_vo.collector_id, collector_vo.domain_id),
        self.create_collector_rules_by_metadata(
            plugin_info["metadata"], collector_vo.collector_id, domain_id
        )

        return collector_vo

    def list_secret_from_secret_filter(
        self,
        secret_filter: dict,
        secret_id: str,
        collector_provider: str,
        domain_id: str,
    ) -> list:
        secret_manager: SecretManager = self.locator.get_manager(SecretManager)

        _filter = self._set_secret_filter(secret_filter, secret_id, collector_provider)
        query = {"filter": _filter} if _filter else {}
        response = secret_manager.list_secrets(query, domain_id)

        return [
            secret_info.get("secret_id") for secret_info in response.get("results", [])
        ]

    @check_required(["schedule"])
    def scheduled_collectors(self, params):
        """Search all collectors in this schedule
        This is global search out-of domain

        Args:
            params(dict): {
                schedule(dict): {
                  'hours': list,
                  'minutes': list
                }
                domain_id: optional
            }

        Returns: collectors_info
        """

        collector_mgr: CollectorManager = self.locator.get_manager(CollectorManager)
        filter_query = [{"k": "schedule.state", "v": "ENABLED", "o": "eq"}]

        if "domain_id" in params:
            filter_query.append({"k": "domain_id", "v": params["domain_id"], "o": "eq"})

        schedule = params.get("schedule", {})
        if "hour" in schedule:
            filter_query.append(
                {"k": "schedule.hours", "v": schedule["hour"], "o": "contain"}
            )

        return collector_mgr.list_collectors({"filter": filter_query})

    def _get_plugin_from_repository(self, plugin_id: str) -> dict:
        repo_mgr: RepositoryManager = self.locator.get_manager(RepositoryManager)
        return repo_mgr.get_plugin(plugin_id)

    def create_collector_rules_by_metadata(
        self,
        collector_rules: list,
        domain_id: str,
        collector_id: str,
        resource_group: str,
        workspace_id: str = None,
    ):
        collector_rule_mgr: CollectorRuleManager = self.locator.get_manager(
            CollectorRuleManager
        )

        for collector_rule_params in collector_rules:
            collector_rule_params.update(
                {
                    "collector_id": collector_id,
                    "rule_type": "MANAGED",
                    "resource_group": resource_group,
                    "workspace_id": workspace_id,
                    "domain_id": domain_id,
                }
            )

            collector_rule_mgr.create_collector_rule(collector_rule_params)

    def delete_collector_rules(self, collector_id: str, domain_id: str) -> None:
        collector_rule_mgr: CollectorRuleManager = self.locator.get_manager(
            "CollectorRuleManager"
        )
        old_collector_rule_vos = collector_rule_mgr.filter_collector_rules(
            collector_id=collector_id, rule_type="MANAGED", domain_id=domain_id
        )
        old_collector_rule_vos.delete()

    @staticmethod
    def _set_secret_filter(
        secret_filter: dict, secret_id: str, collector_provider: str
    ) -> list:
        _filter = []

        if secret_id:
            _filter.append({"k": "secret_id", "v": secret_id, "o": "eq"})

        if collector_provider:
            _filter.append({"k": "provider", "v": collector_provider, "o": "eq"})

        if secret_filter and secret_filter.get("state") == "ENABLED":
            if "secrets" in secret_filter and secret_filter["secrets"]:
                _filter.append(
                    {"k": "secret_id", "v": secret_filter["secrets"], "o": "in"}
                )
            if (
                "service_accounts" in secret_filter
                and secret_filter["service_accounts"]
            ):
                _filter.append(
                    {
                        "k": "service_account_id",
                        "v": secret_filter["service_accounts"],
                        "o": "in",
                    }
                )
            if "schemas" in secret_filter and secret_filter["schemas"]:
                _filter.append(
                    {"k": "schema", "v": secret_filter["schemas"], "o": "in"}
                )
            if "exclude_secrets" in secret_filter and secret_filter["exclude_secrets"]:
                _filter.append(
                    {
                        "k": "secret_id",
                        "v": secret_filter["exclude_secrets"],
                        "o": "not_in",
                    }
                )
            if (
                "exclude_service_accounts" in secret_filter
                and secret_filter["exclude_service_accounts"]
            ):
                _filter.append(
                    {
                        "k": "service_account_id",
                        "v": secret_filter["exclude_service_accounts"],
                        "o": "not_in",
                    }
                )
            if "exclude_schemas" in secret_filter and secret_filter["exclude_schemas"]:
                _filter.append(
                    {
                        "k": "exclude_schemas",
                        "v": secret_filter["exclude_schemas"],
                        "o": "not_in",
                    }
                )

        return _filter

    @staticmethod
    def _get_plugin_providers(provider: str, plugin_info: dict) -> str:
        supported_providers = plugin_info.get("capability", {}).get(
            "supported_providers", []
        )

        if supported_providers:
            # Multi providers
            if provider in supported_providers:
                return provider
            else:
                raise ERROR_INVALID_PARAMETER(
                    key="provider", reason=f"Not supported provider: {provider}"
                )
        else:
            # Single provider
            return provider if provider else plugin_info.get("provider")

    @staticmethod
    def list_projects_from_tasks(tasks: list) -> list:
        projects = []
        for task in tasks:
            if project_id := task["secret_info"].get("project_id"):
                projects.append(project_id)

        return list(set(projects))
