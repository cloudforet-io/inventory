import logging
import copy
import pytz
from datetime import datetime
from typing import List, Union, Tuple

from spaceone.core.service import *
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.manager.change_history_manager import ChangeHistoryManager
from spaceone.inventory.manager.collection_state_manager import CollectionStateManager
from spaceone.inventory.manager.note_manager import NoteManager
from spaceone.inventory.manager.collector_rule_manager import CollectorRuleManager
from spaceone.inventory.manager.export_manager import ExportManager
from spaceone.inventory.error import *

_KEYWORD_FILTER = [
    "cloud_service_id",
    "name",
    "ip_addresses",
    "cloud_service_group",
    "cloud_service_type",
    "reference.resource_id",
]

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceService(BaseService):
    resource = "CloudService"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_mgr: CloudServiceManager = self.locator.get_manager(
            "CloudServiceManager"
        )
        self.region_mgr: RegionManager = self.locator.get_manager("RegionManager")
        self.identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
        self.collector_rule_mgr: CollectorRuleManager = self.locator.get_manager(
            "CollectorRuleManager"
        )
        self.collector_id = self.transaction.get_meta("collector_id")
        self.job_id = self.transaction.get_meta("job_id")
        self.plugin_id = self.transaction.get_meta("plugin_id")
        self.service_account_id = self.transaction.get_meta("secret.service_account_id")

    @transaction(
        permission="inventory:CloudService.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    def create(self, params):
        """
        Args:
            params (dict): {
                'cloud_service_type': 'str',        # required
                'cloud_service_group': 'str',       # required
                'provider': 'str',                  # required
                'name': 'str',
                'account': 'str',
                'instance_type': 'str',
                'instance_size': 'float',
                'ip_addresses': 'list',
                'data': 'dict',                     # required
                'metadata': 'dict',
                'reference': 'dict',
                'tags': 'list or dict',
                'region_code': 'str',
                'project_id': 'str',                # required
                'workspace_id': 'str',              # injected from auth (required)
                'domain_id': 'str'                  # injected from auth (required)
            }

        Returns:
            cloud_service_vo (object)

        """

        return self.create_resource(params)

    @check_required(
        [
            "cloud_service_type",
            "cloud_service_group",
            "provider",
            "data",
            "workspace_id",
            "domain_id",
        ]
    )
    def create_resource(self, params: dict) -> CloudService:
        ch_mgr: ChangeHistoryManager = self.locator.get_manager("ChangeHistoryManager")

        domain_id = params["domain_id"]
        workspace_id = params["workspace_id"]
        secret_project_id = self.transaction.get_meta("secret.project_id")
        provider = params["provider"]

        if instance_size := params.get("instance_size"):
            if not isinstance(instance_size, float):
                raise ERROR_INVALID_PARAMETER_TYPE(key="instance_size", type="float")

        if "tags" in params:
            params["tags"] = self._convert_tags_to_dict(params["tags"])

        # Change data through Collector Rule
        if self._is_created_by_collector():
            params = self.collector_rule_mgr.change_cloud_service_data(
                self.collector_id, domain_id, params
            )

        if "tags" in params:
            params["tags"], params["tag_keys"] = self._convert_tags_to_hash(
                params["tags"], provider
            )

        if "project_id" in params:
            self.identity_mgr.get_project(params["project_id"], domain_id)
        elif secret_project_id:
            params["project_id"] = secret_project_id

        params["ref_cloud_service_type"] = self._make_cloud_service_type_key(params)

        if "region_code" in params:
            params["ref_region"] = self._make_region_key(
                domain_id, workspace_id, provider, params["region_code"]
            )

        if "metadata" in params:
            params["metadata"] = self._convert_metadata(params["metadata"], provider)

        params["collection_info"] = self._get_collection_info()

        cloud_svc_vo = self.cloud_svc_mgr.create_cloud_service(params)

        # Create New History
        ch_mgr.add_new_history(cloud_svc_vo, params)

        # Create Collection State
        state_mgr: CollectionStateManager = self.locator.get_manager(
            "CollectionStateManager"
        )
        state_mgr.create_collection_state(cloud_svc_vo.cloud_service_id, domain_id)

        return cloud_svc_vo

    @transaction(
        permission="inventory:CloudService.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    def update(self, params: dict) -> CloudService:
        """
        Args:
            params (dict): {
                'cloud_service_id': 'str',      # required
                'name': 'str',
                'account': 'str',
                'instance_type': 'str',
                'instance_size': 'float',
                'ip_addresses': 'list',
                'data': 'dict',
                'metadata': 'dict',
                'reference': 'dict',
                'tags': 'list or dict',
                'region_code': 'str',
                'project_id': 'str',
                'workspace_id': 'str',              # injected from auth (required)
                'domain_id': 'str',                 # injected from auth (required)
                'user_projects': 'list'             # injected from auth
            }

        Returns:
            cloud_service_vo (object)
        """

        return self.update_resource(params)

    @check_required(["cloud_service_id", "workspace_id", "domain_id"])
    def update_resource(self, params: dict) -> CloudService:
        ch_mgr: ChangeHistoryManager = self.locator.get_manager("ChangeHistoryManager")

        secret_project_id = self.transaction.get_meta("secret.project_id")

        cloud_service_id = params["cloud_service_id"]
        workspace_id = params["workspace_id"]
        user_projects = params.get("user_projects")
        domain_id = params["domain_id"]
        provider = self._get_provider_from_meta()

        if "ip_addresses" in params and params["ip_addresses"] is None:
            del params["ip_addresses"]

        if instance_size := params.get("instance_size"):
            if not isinstance(instance_size, float):
                raise ERROR_INVALID_PARAMETER_TYPE(key="instance_size", type="float")

        if "tags" in params:
            params["tags"] = self._convert_tags_to_dict(params["tags"])

        # Change data through Collector Rule
        if self._is_created_by_collector():
            params = self.collector_rule_mgr.change_cloud_service_data(
                self.collector_id, domain_id, params
            )

        cloud_svc_vo: CloudService = self.cloud_svc_mgr.get_cloud_service(
            cloud_service_id, domain_id, workspace_id, user_projects
        )

        if "project_id" in params:
            self.identity_mgr.get_project(params["project_id"], domain_id)
        elif secret_project_id and secret_project_id != cloud_svc_vo.project_id:
            params["project_id"] = secret_project_id

        if "region_code" in params:
            params["ref_region"] = self._make_region_key(
                cloud_svc_vo.domain_id,
                cloud_svc_vo.workspace_id,
                cloud_svc_vo.provider,
                params["region_code"],
            )

        old_cloud_svc_data = dict(cloud_svc_vo.to_dict())

        if "tags" in params:
            old_tags = old_cloud_svc_data.get("tags", {})
            old_tag_keys = old_cloud_svc_data.get("tag_keys", {})
            new_tags, new_tag_keys = self._convert_tags_to_hash(
                params["tags"], provider
            )

            if self._is_different_data(new_tags, old_tags, provider):
                old_tags.update(new_tags)
                old_tag_keys.update(new_tag_keys)
                params["tags"] = old_tags
                params["tag_keys"] = old_tag_keys
            else:
                del params["tags"]

        if "metadata" in params:
            old_metadata = old_cloud_svc_data.get("metadata", {})
            new_metadata = self._convert_metadata(params["metadata"], provider)

            if self._is_different_data(new_metadata, old_metadata, provider):
                old_metadata.update(new_metadata)
                params["metadata"] = old_metadata
            else:
                del params["metadata"]

        params["collection_info"] = self._get_collection_info()

        params = self.cloud_svc_mgr.merge_data(params, old_cloud_svc_data)

        cloud_svc_vo = self.cloud_svc_mgr.update_cloud_service_by_vo(
            params, cloud_svc_vo
        )

        # Create Update History
        ch_mgr.add_update_history(cloud_svc_vo, params, old_cloud_svc_data)

        # Update Collection History
        state_mgr: CollectionStateManager = self.locator.get_manager(
            "CollectionStateManager"
        )
        state_vo = state_mgr.get_collection_state(cloud_service_id, domain_id)
        if state_vo:
            state_mgr.reset_collection_state(state_vo)
        else:
            state_mgr.create_collection_state(cloud_service_id, domain_id)

        if "project_id" in params:
            note_mgr: NoteManager = self.locator.get_manager("NoteManager")

            # Update Project ID from Notes
            note_vos = note_mgr.filter_notes(
                cloud_service_id=cloud_service_id, domain_id=domain_id
            )
            note_vos.update(
                {"project_id": params["project_id"], "workspace_id": workspace_id}
            )

        return cloud_svc_vo

    @transaction(
        permission="inventory:CloudService.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    def delete(self, params: dict) -> None:
        self.delete_resource(params)

    @check_required(["cloud_service_id", "domain_id"])
    def delete_resource(self, params: dict) -> None:
        """
        Args:
        params (dict): {
            'cloud_service_id': 'str',      # required
            'workspace_id': 'str',          # injected from auth
            'domain_id': 'str',             # injected from auth (required)
            'user_projects': 'list'         # injected from auth
        }
        Returns:
            None
        """

        ch_mgr: ChangeHistoryManager = self.locator.get_manager("ChangeHistoryManager")

        cloud_service_id = params["cloud_service_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        user_projects = params.get("user_projects")

        cloud_svc_vo: CloudService = self.cloud_svc_mgr.get_cloud_service(
            cloud_service_id, domain_id, workspace_id, user_projects
        )

        self.cloud_svc_mgr.delete_cloud_service_by_vo(cloud_svc_vo)

        # Create Update History
        ch_mgr.add_delete_history(cloud_svc_vo)

        # Cascade Delete Collection State
        state_mgr: CollectionStateManager = self.locator.get_manager(
            "CollectionStateManager"
        )
        state_mgr.delete_collection_state_by_cloud_service_id(
            cloud_service_id, domain_id
        )

    @transaction(
        permission="inventory:CloudService.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["cloud_service_id", "domain_id"])
    def get(self, params: dict) -> CloudService:
        """
        Args:
            params (dict): {
                    'cloud_service_id': 'str',      # required
                    'workspace_id': 'str',          # injected from auth
                    'domain_id': 'str',             # injected from auth (required)
                    'user_projects': 'list'         # injected from auth
                }

        Returns:
            cloud_service_vo (object)

        """

        return self.cloud_svc_mgr.get_cloud_service(
            params["cloud_service_id"],
            params["domain_id"],
            params.get("workspace_id"),
            params.get("user_projects"),
        )

    @transaction(
        permission="inventory:CloudService.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "cloud_service_id",
            "name",
            "state",
            "ip_address",
            "account",
            "instance_type",
            "cloud_service_type",
            "cloud_service_group",
            "provider",
            "region_code",
            "project_id",
            "project_group_id",
            "workspace_id",
            "domain_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def list(self, params: dict):
        """
        Args:
            params (dict): {
                    'query': 'dict (spaceone.api.core.v1.Query)',
                    'cloud_service_id': 'str',
                    'name': 'str',
                    'state': 'str',
                    'ip_address': 'str',
                    'account': 'str',
                    'instance_type': 'str',
                    'cloud_service_type': 'str',
                    'cloud_service_group': 'str',
                    'provider': 'str',
                    'region_code': 'str',
                    'project_id': 'str',
                    'project_group_id': 'str',
                    'workspace_id': 'str',          # injected from auth
                    'domain_id': 'str',             # injected from auth (required)
                    'user_projects': 'list',        # injected from auth
                }

        Returns:
            results (list)
            total_count (int)
        """

        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        query = params.get("query", {})
        reference_filter = {"domain_id": domain_id, "workspace_id": workspace_id}

        return self.cloud_svc_mgr.list_cloud_services(
            query,
            change_filter=True,
            domain_id=domain_id,
            reference_filter=reference_filter,
        )

    @transaction(
        permission="inventory:CloudService.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["options", "domain_id"])
    def export(self, params: dict) -> dict:
        """
        Args:
            params (dict): {
                'options': 'list of ExportOptions (spaceone.api.core.v1.ExportOptions)',    # required
                'file_format': 'str',
                'file_name': 'str',
                'timezone': 'str',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list',    # injected from auth
            }

        Returns:
            download_url (str): URL to download excel

        """

        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        user_projects = params.get("user_projects")
        options = copy.deepcopy(params["options"])
        file_format = params.get("file_format", "EXCEL")
        file_name = params.get("file_name", "cloud_service_export")
        timezone = params.get("timezone", "UTC")

        self._check_timezone(timezone)

        options = self.cloud_svc_mgr.get_export_query_results(
            options, timezone, domain_id, workspace_id, user_projects
        )
        export_mgr: ExportManager = self.locator.get_manager(
            ExportManager, file_format=file_format, file_name=file_name
        )

        return export_mgr.export(options, domain_id, workspace_id)

    @transaction(
        permission="inventory:CloudService.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "query.fields", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id", "user_projects"])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def analyze(self, params: dict) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.AnalyzeQuery)',    # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list',    # injected from auth
            }

        Returns:
            results (list) : 'list of analyze data'

        """

        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        query = params.get("query", {})
        reference_filter = {"domain_id": domain_id, "workspace_id": workspace_id}

        return self.cloud_svc_mgr.analyze_cloud_services(
            query,
            change_filter=True,
            domain_id=params["domain_id"],
            reference_filter=reference_filter,
        )

    @transaction(
        permission="inventory:CloudService.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id", "user_projects"])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params: dict) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
                'user_projects': 'list',    # injected from auth
            }

        Returns:
            results (list) : 'list of statistics data'

        """

        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        query = params.get("query", {})
        reference_filter = {"domain_id": domain_id, "workspace_id": workspace_id}

        return self.cloud_svc_mgr.stat_cloud_services(
            query,
            change_filter=True,
            domain_id=params["domain_id"],
            reference_filter=reference_filter,
        )

    @staticmethod
    def _make_cloud_service_type_key(resource_data: dict) -> str:
        return (
            f'{resource_data["domain_id"]}.{resource_data["workspace_id"]}.{resource_data["provider"]}.'
            f'{resource_data["cloud_service_group"]}.{resource_data["cloud_service_type"]}'
        )

    @staticmethod
    def _make_region_key(
        domain_id: str, workspace_id: str, provider: str, region_code: str
    ) -> str:
        return f"{domain_id}.{workspace_id}.{provider}.{region_code}"

    @staticmethod
    def _convert_metadata(metadata: dict, provider: str) -> dict:
        return {provider: copy.deepcopy(metadata)}

    def _get_collection_info(self) -> dict:
        collector_id = self.transaction.get_meta("collector_id")
        secret_id = self.transaction.get_meta("secret.secret_id")
        service_account_id = self.transaction.get_meta("secret.service_account_id")

        return {
            "collector_id": collector_id,
            "secret_id": secret_id,
            "service_account_id": service_account_id,
            "last_collected_at": datetime.utcnow(),
        }

    @staticmethod
    def _convert_tags_to_dict(tags: Union[list, dict]) -> dict:
        if isinstance(tags, list):
            dot_tags = utils.tags_to_dict(tags)
        elif isinstance(tags, dict):
            dot_tags = copy.deepcopy(tags)
        else:
            dot_tags = {}

        return dot_tags

    @staticmethod
    def _convert_tags_to_hash(dot_tags: dict, provider: str) -> Tuple[dict, dict]:
        tag_keys = {provider: list(dot_tags.keys())}

        tags = {provider: {}}
        for key, value in dot_tags.items():
            hashed_key = utils.string_to_hash(key)
            tags[provider][hashed_key] = {"key": key, "value": value}

        return tags, tag_keys

    @staticmethod
    def _is_different_data(new_data: dict, old_data: dict, provider: str) -> bool:
        if new_data[provider] != old_data.get(provider):
            return True
        else:
            return False

    def _get_provider_from_meta(self) -> str:
        if self._is_created_by_collector():
            return self.transaction.get_meta("secret.provider")
        else:
            return "custom"

    def _is_created_by_collector(self) -> str:
        return (
            self.collector_id
            and self.job_id
            and self.service_account_id
            and self.plugin_id
        )

    @staticmethod
    def _check_timezone(timezone: str) -> None:
        if timezone not in pytz.all_timezones:
            raise ERROR_INVALID_PARAMETER(key="timezone", reason="Timezone is invalid.")
