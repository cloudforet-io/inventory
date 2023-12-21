import logging
from spaceone.core import cache, config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if token := kwargs.get("token"):
            self.identity_conn: SpaceConnector = self.locator.get_connector(
                "SpaceConnector", service="identity", token=token
            )
        else:
            system_token = config.get_global("TOKEN")
            self.identity_conn: SpaceConnector = self.locator.get_connector(
                "SpaceConnector", service="identity", token=system_token
            )

    def check_workspace(self, workspace_id, domain_id):
        return self.identity_conn.dispatch(
            "Workspace.check",
            {"workspace_id": workspace_id, "domain_id": domain_id},
        )

    def get_user(self, user_id: dict) -> dict:
        return self.identity_conn.dispatch("User.get", {"user_id": user_id})

    def list_users(self, query: dict) -> dict:
        return self.identity_conn.dispatch("User.list", {"query": query})

    @cache.cacheable(key="inventory:project:{project_id}", expire=3600)
    def get_project(self, project_id) -> dict:
        return self.identity_conn.dispatch("Project.get", {"project_id": project_id})

    def list_projects(self, query: dict) -> dict:
        return self.identity_conn.dispatch("Project.list", {"query": query})

    @cache.cacheable(
        key="inventory:project:query:{domain_id}:{query_hash}", expire=3600
    )
    def list_projects_with_cache(
        self, query: dict, query_hash: str, domain_id: str
    ) -> dict:
        return self.list_projects(query)

    def list_domains(self, query: dict) -> dict:
        return self.identity_conn.dispatch("Domain.list", {"query": query})

    def list_service_accounts(self, query: dict) -> dict:
        return self.identity_conn.dispatch("ServiceAccount.list", {"query": query})

    def list_schemas(self, query: dict) -> dict:
        return self.identity_conn.dispatch("Schema.list", {"query": query})
