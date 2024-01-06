import logging
from typing import Tuple
from spaceone.core.service import *
from spaceone.core import utils
from spaceone.core.model.mongo_model import QuerySet
from spaceone.inventory.manager.region_manager import RegionManager
from spaceone.inventory.model.region_model import Region

_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ["region_id", "name", "region_code"]


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class RegionService(BaseService):
    resource = "Region"

    def __init__(self, metadata):
        super().__init__(metadata)
        self.region_mgr: RegionManager = self.locator.get_manager("RegionManager")

    @transaction(
        permission="inventory:Region.write",
        role_types=["WORKSPACE_OWNER"],
    )
    def create(self, params: dict) -> Region:
        """
        Args:
        params (dict): {
            'name': 'str',          # required
            'region_code': 'str',   # required
            'provider': 'str',      # required
            'tags': 'dict',
            'workspace_id': 'str',  # injected from auth (required)
            'domain_id': 'str',     # injected from auth (required)
        }
        Returns:
            region_vo (object)
        """

        return self.create_resource(params)

    @check_required(["name", "region_code", "provider", "workspace_id", "domain_id"])
    def create_resource(self, params: dict) -> Region:
        if "tags" in params:
            if isinstance(params["tags"], list):
                params["tags"] = utils.tags_to_dict(params["tags"])

        domain_id = params["domain_id"]
        workspace_id = params["workspace_id"]

        params["updated_by"] = self.transaction.get_meta("collector_id") or "manual"
        params["region_key"] = f'{params["provider"]}.{params["region_code"]}'
        params["ref_region"] = f'{domain_id}.{workspace_id}.{params["region_key"]}'

        return self.region_mgr.create_region(params)

    @transaction(
        permission="inventory:Region.write",
        role_types=["WORKSPACE_OWNER"],
    )
    def update(self, params: dict) -> Region:
        """
        Args:
        params (dict): {
            'region_id': 'str',     # required
            'name': 'str',
            'tags': 'dict',
            'workspace_id': 'str',  # injected from auth (required)
            'domain_id': 'str',     # injected from auth (required)
        }
        Returns:
            region_vo (object)
        """

        return self.update_resource(params)

    @check_required(["region_id", "workspace_id", "domain_id"])
    def update_resource(self, params: dict) -> Region:
        if "tags" in params:
            if isinstance(params["tags"], list):
                params["tags"] = utils.tags_to_dict(params["tags"])

        params["updated_by"] = self.transaction.get_meta("collector_id") or "manual"

        region_vo = self.region_mgr.get_region(
            params["region_id"], params["domain_id"], params["workspace_id"]
        )
        return self.region_mgr.update_region_by_vo(params, region_vo)

    @transaction(
        permission="inventory:Region.write",
        role_types=["WORKSPACE_OWNER"],
    )
    def delete(self, params: dict) -> None:
        """
        Args:
        params (dict): {
            'region_id': 'str',     # required
            'workspace_id': 'str',  # injected from auth (required)
            'domain_id': 'str'      # injected from auth (required)
        }
        Returns:
            None
        """

        self.delete_resource(params)

    @check_required(["region_id", "workspace_id", "domain_id"])
    def delete_resource(self, params: dict) -> None:
        region_vo = self.region_mgr.get_region(
            params["region_id"], params["domain_id"], params["workspace_id"]
        )
        self.region_mgr.delete_region_by_vo(region_vo)

    @transaction(
        permission="inventory:Region.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["region_id", "domain_id"])
    def get(self, params: dict) -> Region:
        """
        Args:
            params (dict): {
                'region_id': 'str',     # required
                'workspace_id': 'str',  # injected from auth
                'domain_id': 'str',     # injected from auth (required)
            }

        Returns:
            region_vo (object)

        """

        return self.region_mgr.get_region(
            params["region_id"], params["domain_id"], params.get("workspace_id")
        )

    @transaction(
        permission="inventory:Region.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "region_id",
            "name",
            "region_key",
            "region_code",
            "provider",
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
                'region_id': 'str',
                'name': 'str',
                'region_key': 'str',
                'region_code': 'str',
                'provider': 'str',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            results (list)
            total_count (int)

        """

        return self.region_mgr.list_regions(params.get("query", {}))

    @transaction(
        permission="inventory:Region.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params: dict) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',     # required
                "workspace_id": "str",  # injected from auth
                'domain_id': 'str',     # injected from auth (required)
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get("query", {})
        return self.region_mgr.stat_regions(query)
