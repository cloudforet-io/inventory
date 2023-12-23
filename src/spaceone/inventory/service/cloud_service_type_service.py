from typing import Tuple

from spaceone.core import utils
from spaceone.core.service import *
from spaceone.core.model.mongo_model import QuerySet
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.manager.cloud_service_type_manager import (
    CloudServiceTypeManager,
)


_KEYWORD_FILTER = ["cloud_service_type_id", "name", "group", "service_code"]


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceTypeService(BaseService):
    resource = "CloudServiceType"

    def __init__(self, metadata):
        super().__init__(metadata)
        self.cloud_svc_type_mgr: CloudServiceTypeManager = self.locator.get_manager(
            "CloudServiceTypeManager"
        )

    @transaction(
        permission="inventory:CloudServiceType.write",
        role_types=["WORKSPACE_OWNER"],
    )
    def create(self, params: dict) -> CloudServiceType:
        """
        Args:
            params (dict): {
                'name': 'str',              # required
                'group': 'str',             # required
                'provider': 'str',          # required
                'service_code': 'str',
                'is_primary': 'bool',
                'is_major': 'bool',
                'resource_type': 'str',
                'metadata': 'dict',
                'labels': 'list,
                'tags': 'dict',
                'workspace_id': 'str',      # injected from auth (required)
                'domain_id': 'str'          # injected from auth (required)
            }

        Returns:
            cloud_service_type_vo (object)
        """

        return self.create_resource(params)

    @check_required(["name", "group", "provider", "workspace_id", "domain_id"])
    def create_resource(self, params: dict) -> CloudServiceType:
        if "tags" in params:
            if isinstance(params["tags"], list):
                params["tags"] = utils.tags_to_dict(params["tags"])

        params["updated_by"] = self.transaction.get_meta("collector_id") or "manual"

        provider = params.get("provider", self.transaction.get_meta("secret.provider"))

        if provider:
            params["provider"] = provider

        params["resource_type"] = params.get("resource_type", "inventory.CloudService")

        params["ref_cloud_service_type"] = (
            f'{params["domain_id"]}.{params["workspace_id"]}.{params["provider"]}.'
            f'{params["group"]}.{params["name"]}'
        )

        params[
            "cloud_service_type_key"
        ] = f'{params["provider"]}.{params["group"]}.{params["name"]}'

        return self.cloud_svc_type_mgr.create_cloud_service_type(params)

    @transaction(
        permission="inventory:CloudServiceType.write",
        role_types=["WORKSPACE_OWNER"],
    )
    def update(self, params: dict) -> CloudServiceType:
        """
        Args:
            params (dict): {
                'cloud_service_type_id': 'str',     # required
                'service_code': 'str',
                'is_primary': 'bool',
                'is_major': 'bool',
                'resource_type': 'str',
                'metadata': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'workspace_id': 'str',              # injected from auth (required)
                'domain_id': 'str'                  # injected from auth (required)
            }

        Returns:
            cloud_service_type_vo (object)
        """

        return self.update_resource(params)

    @check_required(["cloud_service_type_id", "workspace_id", "domain_id"])
    def update_resource(self, params: dict) -> CloudServiceType:
        if "tags" in params:
            if isinstance(params["tags"], list):
                params["tags"] = utils.tags_to_dict(params["tags"])

        params["updated_by"] = self.transaction.get_meta("collector_id") or "manual"
        domain_id = params["domain_id"]

        cloud_svc_type_vo = self.cloud_svc_type_mgr.get_cloud_service_type(
            params["cloud_service_type_id"], domain_id
        )

        return self.cloud_svc_type_mgr.update_cloud_service_type_by_vo(
            params, cloud_svc_type_vo
        )

    @transaction(
        permission="inventory:CloudServiceType.write",
        role_types=["WORKSPACE_OWNER"],
    )
    def delete(self, params: dict) -> None:
        """
        Args:
        params (dict): {
            'cloud_service_type_id': 'str',     # required
            'workspace_id': 'str',              # injected from auth (required)
            'domain_id': 'str'                  # injected from auth (required)
        }
        Returns:
            None
        """

        self.delete_resource(params)

    @check_required(["cloud_service_type_id", "workspace_id", "domain_id"])
    def delete_resource(self, params: dict) -> None:
        cloud_svc_type_vo = self.cloud_svc_type_mgr.get_cloud_service_type(
            params["cloud_service_type_id"], params["domain_id"], params["workspace_id"]
        )

        self.cloud_svc_type_mgr.delete_cloud_service_type_by_vo(cloud_svc_type_vo)

    @transaction(
        permission="inventory:CloudServiceType.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["cloud_service_type_id", "domain_id"])
    def get(self, params: dict) -> CloudServiceType:
        """
        Args:
            params (dict): {
                'cloud_service_type_id': 'str',     # required
                'workspace_id': 'str',              # injected from auth
                'domain_id': 'str',                 # injected from auth (required)
            }

        Returns:
            cloud_service_type_vo (object)

        """

        return self.cloud_svc_type_mgr.get_cloud_service_type(
            params["cloud_service_type_id"],
            params["domain_id"],
            params.get("workspace_id"),
        )

    @transaction(
        permission="inventory:CloudServiceType.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "cloud_service_type_id",
            "name",
            "provider",
            "group",
            "cloud_service_type_key",
            "service_code",
            "is_primary",
            "is_major",
            "resource_type",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(_KEYWORD_FILTER)
    def list(self, params: dict) -> Tuple[QuerySet, int]:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'cloud_service_type_id': 'str',
                'name': 'str',
                'group': 'str',
                'provider': 'str',
                'cloud_service_type_key': 'str',
                'service_code': 'str',
                'is_primary': 'str',
                'is_major': 'str',
                'resource_type': 'str',
                'workspace_id': 'str',              # injected from auth
                'domain_id': 'str',                 # injected from auth (required)
            }

        Returns:
            results (list)
            total_count (int)

        """

        return self.cloud_svc_type_mgr.list_cloud_service_types(params.get("query", {}))

    @transaction(
        permission="inventory:CloudServiceType.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params: dict) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'workspace_id': 'str',              # injected from auth
                'domain_id': 'str',                 # injected from auth (required)
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get("query", {})
        return self.cloud_svc_type_mgr.stat_cloud_service_types(query)
