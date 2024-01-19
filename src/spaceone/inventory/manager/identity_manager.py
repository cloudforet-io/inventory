import logging
from spaceone.core import cache, config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core.auth.jwt.jwt_util import JWTUtil

_LOGGER = logging.getLogger(__name__)


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="identity"
        )

    def check_workspace(self, workspace_id: str, domain_id: str) -> None:
        system_token = config.get_global("TOKEN")

        return self.identity_conn.dispatch(
            "Workspace.check",
            {"workspace_id": workspace_id, "domain_id": domain_id},
            token=system_token,
        )

    @cache.cacheable(key="inventory:project:{domain_id}:{project_id}", expire=3600)
    def get_project(self, project_id, domain_id) -> dict:
        token = self.transaction.get_meta("token")
        token_type = JWTUtil.get_value_from_token(token, "typ")

        if token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "Project.get",
                {"project_id": project_id},
                x_domain_id=domain_id,
            )
        else:
            return self.identity_conn.dispatch(
                "Project.get", {"project_id": project_id}
            )

    def list_projects(self, params: dict, domain_id: str) -> dict:
        token = self.transaction.get_meta("token")
        token_type = JWTUtil.get_value_from_token(token, "typ")

        if token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "Project.list",
                params,
                x_domain_id=domain_id,
            )
        else:
            return self.identity_conn.dispatch("Project.list", params)

    def list_project_groups(
        self,
        params: dict,
        domain_id: str,
    ) -> dict:
        token = self.transaction.get_meta("token")
        token_type = JWTUtil.get_value_from_token(token, "typ")
        if token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "ProjectGroup.list",
                params,
                x_domain_id=domain_id,
            )
        else:
            return self.identity_conn.dispatch("ProjectGroup.list", params)

    @cache.cacheable(key="inventory:projects-in-pg:{project_group_id}", expire=300)
    def get_projects_in_project_group(self, project_group_id: str):
        params = {
            "query": {
                "only": ["project_id"],
            },
            "project_group_id": project_group_id,
            "include_children": True,
        }

        return self.identity_conn.dispatch("Project.list", params)

    @cache.cacheable(
        key="inventory:project:query:{domain_id}:{query_hash}", expire=3600
    )
    def list_projects_with_cache(
        self, query: dict, query_hash: str, domain_id: str
    ) -> dict:
        return self.list_projects({"query": query}, domain_id)

    def list_service_accounts(self, query: dict, domain_id: str) -> dict:
        token = self.transaction.get_meta("token")
        token_type = JWTUtil.get_value_from_token(token, "typ")

        if token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "ServiceAccount.list",
                {"query": query},
                x_domain_id=domain_id,
            )
        else:
            return self.identity_conn.dispatch("ServiceAccount.list", {"query": query})

    @cache.cacheable(
        key="inventory:service-account:query:{domain_id}:{query_hash}", expire=3600
    )
    def list_service_accounts_with_cache(
        self, query: dict, query_hash: str, domain_id: str
    ) -> dict:
        return self.list_service_accounts(query, domain_id)

    def list_schemas(self, query: dict, domain_id: str) -> dict:
        # For general user, use access token
        return self.identity_conn.dispatch("Schema.list", {"query": query})

    def list_domains(self, query: dict) -> dict:
        # For background job, use system token
        return self.identity_conn.dispatch(
            "Domain.list", {"query": query, "state": "ENABLED"}
        )
