import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.service.utils import *
from spaceone.core.error import *

from spaceone.inventory.model.namespace.request import *
from spaceone.inventory.model.namespace.response import *
from spaceone.inventory.manager.namespace_manager import NamespaceManager
from spaceone.inventory.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class NamespaceService(BaseService):
    resource = "Namespace"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace_mgr = NamespaceManager()
        self.identity_mgr = IdentityManager()

    @transaction(
        permission="inventory:Namespace.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def create(self, params: NamespaceCreateRequest) -> Union[NamespaceResponse, dict]:
        """Create namespace

        Args:
            params (dict): {
                'namespace_id': 'str',
                'name': 'str',                  # required
                'category': 'str',              # required
                'resource_type': 'str',         # required
                'group': 'str',
                'icon': 'str',
                'tags': 'dict',
                'resource_group': 'str',        # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            NamespaceResponse:
        """

        if params.resource_group == "WORKSPACE":
            if not params.workspace_id:
                raise ERROR_REQUIRED_PARAMETER(key="workspace_id")

            self.identity_mgr.check_workspace(params.workspace_id, params.domain_id)
        else:
            params.workspace_id = "*"

        namespace_vo = self.namespace_mgr.create_namespace(params.dict())
        return NamespaceResponse(**namespace_vo.to_dict())

    @transaction(
        permission="inventory:Namespace.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def update(self, params: NamespaceUpdateRequest) -> Union[NamespaceResponse, dict]:
        """Update namespace

        Args:
            params (dict): {
                'namespace_id': 'str',          # required
                'name': 'str',
                'icon': 'str',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            NamespaceResponse:
        """

        namespace_vo = self.namespace_mgr.get_namespace(
            params.namespace_id,
            params.domain_id,
            params.workspace_id,
        )

        if namespace_vo.is_managed:
            raise ERROR_PERMISSION_DENIED()

        namespace_vo = self.namespace_mgr.update_namespace_by_vo(
            params.dict(exclude_unset=True), namespace_vo
        )

        return NamespaceResponse(**namespace_vo.to_dict())

    @transaction(
        permission="inventory:Namespace.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @convert_model
    def delete(self, params: NamespaceDeleteRequest) -> None:
        """Delete namespace

        Args:
            params (dict): {
                'namespace_id': 'str',          # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            None
        """

        namespace_vo = self.namespace_mgr.get_namespace(
            params.namespace_id,
            params.domain_id,
            params.workspace_id,
        )

        if namespace_vo.is_managed:
            raise ERROR_PERMISSION_DENIED()

        self.namespace_mgr.delete_namespace_by_vo(namespace_vo)

    @transaction(
        permission="inventory:Namespace.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @convert_model
    def get(self, params: NamespaceGetRequest) -> Union[NamespaceResponse, dict]:
        """Get namespace

        Args:
            params (dict): {
                'namespace_id': 'str',          # required
                'workspace_id': 'list',         # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            NamespaceResponse:
        """

        namespace_vo = self.namespace_mgr.get_namespace(
            params.namespace_id,
            params.domain_id,
            params.workspace_id,
        )

        return NamespaceResponse(**namespace_vo.to_dict())

    @transaction(
        permission="inventory:Namespace.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @append_query_filter(
        [
            "namespace_id",
            "category",
            "resource_type",
            "group",
            "is_managed",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["namespace_id", "name"])
    @convert_model
    def list(
        self, params: NamespaceSearchQueryRequest
    ) -> Union[NamespacesResponse, dict]:
        """List namespaces

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'namespace_id': 'str',
                'category': 'str',
                'resource_type': 'str',
                'group': 'str',
                'is_managed': 'bool',
                'workspace_id': 'list',         # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            NamespacesResponse:
        """

        query = params.query or {}
        namespace_vos, total_count = self.namespace_mgr.list_namespaces(
            query, params.domain_id
        )

        namespaces_info = [namespace_vo.to_dict() for namespace_vo in namespace_vos]
        return NamespacesResponse(results=namespaces_info, total_count=total_count)

    @transaction(
        permission="inventory:Namespace.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(["namespace_id", "name"])
    @convert_model
    def stat(self, params: NamespaceStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)', # required
                'workspace_id': 'list',     # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            dict: {
                'results': 'list',
                'total_count': 'int'
            }
        """

        query = params.query or {}
        return self.namespace_mgr.stat_namespaces(query)
