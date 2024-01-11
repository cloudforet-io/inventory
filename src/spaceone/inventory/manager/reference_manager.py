import logging
from spaceone.core.manager import BaseManager
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)


class ReferenceManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._project_map = None
        self._service_account_map = None
        self._region_map = None

    def get_reference_name(
        self,
        resource_type: str,
        resource_id: str,
        domain_id: str,
        workspace_id: str = None,
    ) -> str:
        if resource_type == "identity.Project":
            if self._project_map is None:
                self._init_project(domain_id)

            return self._project_map.get(resource_id, resource_id)

        elif resource_type == "identity.ServiceAccount":
            if self._service_account_map is None:
                self._init_service_account(domain_id)

            return self._service_account_map.get(resource_id, resource_id)

        elif resource_type == "inventory.Region":
            if self._region_map is None:
                self._init_region(domain_id, workspace_id)

            return self._region_map.get(resource_id, resource_id)

        else:
            return resource_id

    def _init_project(self, domain_id: str) -> None:
        self._project_map = {}
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")

        query = {"only": ["project_id", "name"]}

        response = identity_mgr.list_projects({"query": query}, domain_id)
        for project_info in response.get("results", []):
            project_id = project_info["project_id"]
            project_name = project_info["name"]
            self._project_map[project_id] = project_name

    def _init_service_account(self, domain_id: str) -> None:
        self._service_account_map = {}
        identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")

        query = {"only": ["service_account_id", "name"]}

        response = identity_mgr.list_service_accounts(query, domain_id)
        for sa_info in response.get("results", []):
            sa_id = sa_info["service_account_id"]
            sa_name = sa_info["name"]
            self._service_account_map[sa_id] = sa_name

    def _init_region(self, domain_id: str, workspace_id: str = None) -> None:
        self._region_map = {}
        region_mgr: RegionManager = self.locator.get_manager("RegionManager")

        conditions = {
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        region_vos = region_mgr.filter_regions(**conditions)
        for region_vo in region_vos:
            region_code = region_vo.region_code
            name = region_vo.name

            self._region_map[region_vo.region_code] = f"{name} | {region_code}"
