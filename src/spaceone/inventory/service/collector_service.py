import logging
import copy
from typing import Tuple
from spaceone.core.service import *
from spaceone.core.model.mongo_model import QuerySet
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
from spaceone.inventory.model.job_model import Job

_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ["collector_id", "name", "provider"]


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CollectorService(BaseService):
    resource = "Collector"

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
        secret_mgr: SecretManager = self.locator.get_manager(SecretManager)

        # Check permission by resource group
        if params["resource_group"] == "WORKSPACE":
            if "workspace_id" not in params:
                raise ERROR_REQUIRED_PARAMETER(key="workspace_id")

            identity_mgr.check_workspace(params["workspace_id"], params["domain_id"])
        else:
            params["workspace_id"] = "*"

        if schedule := params.get("schedule"):
            self._check_schedule(schedule)

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
            if params["secret_filter"].get("state") == "ENABLED":
                self._validate_secret_filter(
                    identity_mgr,
                    secret_mgr,
                    params["secret_filter"],
                    plugin_provider,
                    domain_id,
                )
            else:
                del params["secret_filter"]

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
            params["resource_group"],
            params["domain_id"],
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

        domain_id = params["domain_id"]

        if schedule := params.get("schedule"):
            self._check_schedule(schedule)

        collector_vo = self.collector_mgr.get_collector(
            params["collector_id"], params["domain_id"], params.get("workspace_id")
        )

        if "secret_filter" in params:
            if params["secret_filter"].get("state") == "ENABLED":
                identity_mgr: IdentityManager = self.locator.get_manager(
                    IdentityManager
                )
                secret_mgr: SecretManager = self.locator.get_manager(SecretManager)

                self._validate_secret_filter(
                    identity_mgr,
                    secret_mgr,
                    params["secret_filter"],
                    collector_vo.provider,
                    domain_id,
                )
            else:
                params["secret_filter"] = {
                    "state": "DISABLED",
                }

        return self.collector_mgr.update_collector_by_vo(collector_vo, params)

    @transaction(
        permission="inventory:Collector.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_id", "domain_id"])
    def update_plugin(self, params: dict) -> Collector:
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
            domain_id,
            plugin_info.get("upgrade_mode", "AUTO"),
            plugin_info.get("version"),
        )

        collector_vo = self._update_collector_plugin(
            endpoint, updated_version, plugin_info, collector_vo
        )
        return collector_vo

    @transaction(
        permission="inventory:Collector.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_id", "domain_id"])
    def verify_plugin(self, params: dict) -> None:
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
            plugin_info.get("upgrade_mode", "AUTO"),
            domain_id,
        )

        secret_ids = self._get_secret_ids_from_filter(
            collector_vo.secret_filter.to_dict(),
            collector_vo.provider,
            domain_id,
            params.get("secret_id"),
        )

        if secret_ids:
            secret_data_info = secret_manager.get_secret_data(secret_ids[0], domain_id)
            secret_data = secret_data_info.get("data", {})
            collector_plugin_mgr.verify_plugin(
                endpoint, plugin_info.get("options", {}), secret_data
            )

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
        job_mgr: JobManager = self.locator.get_manager(JobManager)
        job_task_mgr: JobTaskManager = self.locator.get_manager(JobTaskManager)

        collector_vo: Collector = self.collector_mgr.get_collector(
            params["collector_id"], params["domain_id"], params.get("workspace_id")
        )

        state_mgr.delete_collection_state_by_collector_id(
            params["collector_id"], params["domain_id"]
        )

        job_vos = job_mgr.filter_jobs(
            collector_id=params["collector_id"], domain_id=params["domain_id"]
        )
        job_vos.delete()

        job_task_vos = job_task_mgr.filter_job_tasks(
            collector_id=params["collector_id"], domain_id=params["domain_id"]
        )
        job_task_vos.delete()

        self.collector_mgr.delete_collector_by_vo(collector_vo)

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
        workspace_id = params.get("workspace_id")

        return collector_mgr.get_collector(collector_id, domain_id, workspace_id)

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
            "secret_filter_state",
            "schedule_state",
            "plugin_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(_KEYWORD_FILTER)
    def list(self, params: dict) -> Tuple[QuerySet, int]:
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
    def stat(self, params: dict) -> dict:
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
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @check_required(["collector_id", "domain_id"])
    def collect(self, params: dict) -> Job:
        """Collect data
        Args:
            params (dict): {
                'collector_id': 'str',      # required
                'secret_id': 'str',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list',    # injected from auth
            }

        Returns:
            job_vo (object)
        """

        plugin_mgr: PluginManager = self.locator.get_manager(PluginManager)
        job_mgr: JobManager = self.locator.get_manager(JobManager)
        job_task_mgr: JobTaskManager = self.locator.get_manager(JobTaskManager)

        collector_id = params["collector_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        collector_vo = self.collector_mgr.get_collector(
            collector_id, domain_id, workspace_id
        )
        collector_data = collector_vo.to_dict()

        plugin_info = collector_data["plugin_info"]
        secret_filter = collector_data.get("secret_filter", {}) or {}
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
                endpoint, updated_version, plugin_info, collector_vo
            )

        tasks = self._get_tasks(
            params,
            endpoint,
            collector_vo.provider,
            plugin_info,
            secret_filter,
            domain_id,
        )

        duplicated_job_vos = job_mgr.get_duplicate_jobs(
            collector_id, domain_id, workspace_id, params.get("secret_id")
        )

        for job_vo in duplicated_job_vos:
            job_mgr.make_canceled_by_vo(job_vo)

        # create job
        params["plugin_id"] = plugin_id
        params["total_tasks"] = len(tasks)
        params["remained_tasks"] = len(tasks)
        job_vo = job_mgr.create_job(collector_vo, params)

        if len(tasks) > 0:
            for task in tasks:
                secret_info = task["secret_info"]
                sub_tasks = task.get("sub_tasks", [])
                if len(sub_tasks) == 0:
                    sub_task_count = 1
                else:
                    sub_task_count = len(sub_tasks)

                del task["sub_tasks"]

                create_params = {
                    "total_sub_tasks": sub_task_count,
                    "remained_sub_tasks": sub_task_count,
                    "job_id": job_vo.job_id,
                    "collector_id": job_vo.collector_id,
                    "secret_id": secret_info.get("secret_id"),
                    "service_account_id": secret_info.get("service_account_id"),
                    "project_id": secret_info.get("project_id"),
                    "workspace_id": secret_info.get("workspace_id"),
                    "domain_id": domain_id,
                    "options": task["task_options"],
                }

                task.update({"collector_id": collector_id, "job_id": job_vo.job_id})

                try:
                    # create job task
                    job_task_vo = job_task_mgr.create_job_task(create_params)
                    task.update({"job_task_id": job_task_vo.job_task_id})

                    if sub_task_count > 0:
                        for sub_task in sub_tasks:
                            task.update({"task_options": sub_task, "is_sub_task": True})
                            job_task_mgr.push_job_task(task)
                    else:
                        job_task_mgr.push_job_task(task)

                except Exception as e:
                    _LOGGER.error(
                        f"[collect] Error to create job task ({job_vo.job_id}): {e}",
                        exc_info=True,
                    )
                    job_mgr.make_failure_by_vo(job_vo)
        else:
            # close job if no tasks
            job_mgr.make_success_by_vo(job_vo)
            return job_vo

        self.collector_mgr.update_last_collected_time(collector_vo)
        return job_vo

    def _get_tasks(
        self,
        params: dict,
        endpoint: str,
        collector_provider: str,
        plugin_info: dict,
        secret_filter: dict,
        domain_id: str,
    ) -> list:
        secret_mgr: SecretManager = self.locator.get_manager(SecretManager)
        collector_plugin_mgr: CollectorPluginManager = self.locator.get_manager(
            CollectorPluginManager
        )

        tasks = []
        secret_ids = self._get_secret_ids_from_filter(
            secret_filter,
            collector_provider,
            domain_id,
            params.get("secret_id"),
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
                    endpoint,
                    secret_data.get("data", {}),
                    plugin_info.get("options", {}),
                )
                _LOGGER.debug(f"[get_tasks] sub tasks: {response}")
                _task["sub_tasks"] = response.get("tasks", [])

            except Exception as e:
                pass

            tasks.append(_task)

        return tasks

    @staticmethod
    def _check_secrets(
        secret_mgr: SecretManager, secret_ids: list, provider: str, domain_id: str
    ) -> None:
        query = {
            "filter": [
                {"k": "secret_id", "v": secret_ids, "o": "in"},
                {"k": "provider", "v": provider, "o": "eq"},
            ],
            "count_only": True,
        }
        response = secret_mgr.list_secrets(query, domain_id)
        total_count = response.get("total_count", 0)

        if total_count != len(secret_ids):
            raise ERROR_INVALID_PARAMETER(
                key="secret_filter.secrets",
                reason=f"secrets are not found: {', '.join(secret_ids)}",
            )

    @staticmethod
    def _check_service_accounts(
        identity_mgr: IdentityManager,
        service_account_ids: list,
        provider: str,
        domain_id: str,
    ) -> None:
        query = {
            "filter": [
                {
                    "k": "service_account_id",
                    "v": service_account_ids,
                    "o": "in",
                },
                {"k": "provider", "v": provider, "o": "eq"},
            ],
            "count_only": True,
        }

        response = identity_mgr.list_service_accounts(query, domain_id)
        total_count = response.get("total_count", 0)

        if total_count != len(service_account_ids):
            raise ERROR_INVALID_PARAMETER(
                key="secret_filter.service_accounts",
                reason=f"service accounts are not found: {', '.join(service_account_ids)}",
            )

    @staticmethod
    def _check_schemas(
        identity_mgr: IdentityManager,
        schema_ids: list,
        provider: str,
        domain_id: str,
    ) -> None:
        query = {
            "filter": [
                {
                    "k": "schema_id",
                    "v": schema_ids,
                    "o": "in",
                },
                {"k": "provider", "v": provider, "o": "eq"},
            ],
            "count_only": True,
        }

        response = identity_mgr.list_schemas(query, domain_id)
        total_count = response.get("total_count", 0)

        if total_count != len(schema_ids):
            raise ERROR_INVALID_PARAMETER(
                key="secret_filter.schemas",
                reason=f"schemas are not found: {', '.join(schema_ids)}",
            )

    def _validate_secret_filter(
        self,
        identity_mgr: IdentityManager,
        secret_mgr: SecretManager,
        secret_filter: dict,
        provider: str,
        domain_id: str,
    ) -> None:
        if "secrets" in secret_filter:
            self._check_secrets(
                secret_mgr, secret_filter["secrets"], provider, domain_id
            )

        if "service_accounts" in secret_filter:
            self._check_service_accounts(
                identity_mgr, secret_filter["service_accounts"], provider, domain_id
            )

        if "schemas" in secret_filter:
            self._check_schemas(
                identity_mgr, secret_filter["schemas"], provider, domain_id
            )

        if "exclude_secrets" in secret_filter:
            self._check_secrets(
                secret_mgr, secret_filter["exclude_secrets"], provider, domain_id
            )

        if "exclude_service_accounts" in secret_filter:
            self._check_service_accounts(
                identity_mgr,
                secret_filter["exclude_service_accounts"],
                provider,
                domain_id,
            )

        if "exclude_schemas" in secret_filter:
            self._check_schemas(
                identity_mgr, secret_filter["exclude_schemas"], provider, domain_id
            )

    def _update_collector_plugin(
        self,
        endpoint: str,
        updated_version: str,
        plugin_info: dict,
        collector_vo: Collector,
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

        collector_vo = self.collector_mgr.update_collector_by_vo(
            collector_vo, {"plugin_info": plugin_info}
        )

        self.delete_collector_rules(collector_vo.collector_id, collector_vo.domain_id),

        collector_rules = plugin_info["metadata"].get("collector_rules", [])
        self.create_collector_rules_by_metadata(
            collector_rules,
            collector_vo.collector_id,
            collector_vo.resource_group,
            collector_vo.domain_id,
            collector_vo.workspace_id,
        )

        return collector_vo

    def _get_secret_ids_from_filter(
        self,
        secret_filter: dict,
        provider: str,
        domain_id: str,
        secret_id: str = None,
    ) -> list:
        secret_manager: SecretManager = self.locator.get_manager(SecretManager)

        query = {"filter": self._make_secret_filter(secret_filter, provider, secret_id)}
        response = secret_manager.list_secrets(query, domain_id)

        return [
            secret_info.get("secret_id") for secret_info in response.get("results", [])
        ]

    @check_required(["hour"])
    def scheduled_collectors(self, params: dict) -> Tuple[QuerySet, int]:
        """Search all collectors in this schedule.
        This is global search out-of domain.

        Args:
            params(dict): {
                'hour': 'int',        # required
            }

        Returns:
            results (list)
            total_count (int)
        """

        collector_mgr: CollectorManager = self.locator.get_manager(CollectorManager)
        query = {
            "filter": [
                {"k": "schedule.state", "v": "ENABLED", "o": "eq"},
                {"k": "schedule.hours", "v": params["hour"], "o": "contain"},
            ]
        }
        return collector_mgr.list_collectors(query)

    def _get_plugin_from_repository(self, plugin_id: str) -> dict:
        repo_mgr: RepositoryManager = self.locator.get_manager(RepositoryManager)
        return repo_mgr.get_plugin(plugin_id)

    def create_collector_rules_by_metadata(
        self,
        collector_rules: list,
        collector_id: str,
        resource_group: str,
        domain_id: str,
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
    def _make_secret_filter(
        secret_filter: dict, provider: str, secret_id: str = None
    ) -> list:
        _filter = [{"k": "provider", "v": provider, "o": "eq"}]

        if secret_id:
            _filter.append({"k": "secret_id", "v": secret_id, "o": "eq"})

        if secret_filter.get("state") == "ENABLED":
            if secrets := secret_filter.get("secrets"):
                _filter.append({"k": "secret_id", "v": secrets, "o": "in"})

            if service_accounts := secret_filter.get("service_accounts"):
                _filter.append(
                    {"k": "service_account_id", "v": service_accounts, "o": "in"}
                )

            if schemas := secret_filter.get("schemas"):
                _filter.append({"k": "schema", "v": schemas, "o": "in"})

            if exclude_secrets := secret_filter.get("exclude_secrets"):
                _filter.append({"k": "secret_id", "v": exclude_secrets, "o": "not_in"})

            if exclude_service_accounts := secret_filter.get(
                "exclude_service_accounts"
            ):
                _filter.append(
                    {
                        "k": "service_account_id",
                        "v": exclude_service_accounts,
                        "o": "not_in",
                    }
                )

            if exclude_schemas := secret_filter.get("exclude_schemas"):
                _filter.append({"k": "schema", "v": exclude_schemas, "o": "not_in"})

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
    def _check_schedule(schedule: dict) -> None:
        if schedule.get("state") == "ENABLED":
            if hours := schedule.get("hours"):
                if len(hours) > 2:
                    raise ERROR_INVALID_PARAMETER(
                        key="schedule.hours", reason="Maximum 2 hours can be set."
                    )
