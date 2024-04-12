import logging
from typing import Tuple

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.core import utils, cache
from spaceone.inventory.model.namespace.database import Namespace
from spaceone.inventory.manager.managed_resource_manager import ManagedResourceManager

_LOGGER = logging.getLogger(__name__)


class NamespaceManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace_model = Namespace

    def create_namespace(self, params: dict) -> Namespace:
        def _rollback(vo: Namespace):
            _LOGGER.info(
                f"[create_namespace._rollback] " f"Delete namespace: {vo.namespace_id}"
            )
            vo.delete()

        if "namespace_id" not in params:
            params["namespace_id"] = utils.generate_id("ns")

        params["workspaces"] = [params["workspace_id"]]

        namespace_vo: Namespace = self.namespace_model.create(params)
        self.transaction.add_rollback(_rollback, namespace_vo)

        return namespace_vo

    def update_namespace_by_vo(
        self, params: dict, namespace_vo: Namespace
    ) -> Namespace:
        def _rollback(old_data):
            _LOGGER.info(
                f"[update_namespace_by_vo._rollback] Revert Data: "
                f'{old_data["namespace_id"]}'
            )
            namespace_vo.update(old_data)

        self.transaction.add_rollback(_rollback, namespace_vo.to_dict())

        return namespace_vo.update(params)

    @staticmethod
    def delete_namespace_by_vo(namespace_vo: Namespace) -> None:
        namespace_vo.delete()

    def get_namespace(
        self,
        namespace_id: str,
        domain_id: str,
        workspace_id: str = None,
    ) -> Namespace:
        conditions = {
            "namespace_id": namespace_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspaces"] = [workspace_id, "*"]

        return self.namespace_model.get(**conditions)

    def filter_namespaces(self, **conditions) -> QuerySet:
        return self.namespace_model.filter(**conditions)

    def list_namespaces(self, query: dict, domain_id: str) -> Tuple[QuerySet, int]:
        self._create_managed_namespace(domain_id)
        return self.namespace_model.query(**query)

    def stat_namespaces(self, query: dict) -> dict:
        return self.namespace_model.stat(**query)

    @cache.cacheable(key="inventory:managed-namespace:{domain_id}:sync", expire=300)
    def _create_managed_namespace(self, domain_id: str) -> bool:
        managed_resource_mgr = ManagedResourceManager()

        namespace_vos = self.filter_namespaces(domain_id=domain_id, is_managed=True)

        installed_namespace_version_map = {}
        for namespace_vo in namespace_vos:
            installed_namespace_version_map[
                namespace_vo.namespace_id
            ] = namespace_vo.version

        managed_namespace_map = managed_resource_mgr.get_managed_namespaces()

        for managed_ns_id, managed_ns_info in managed_namespace_map.items():
            managed_ns_info["domain_id"] = domain_id
            managed_ns_info["is_managed"] = True
            managed_ns_info["workspace_id"] = "*"

            if ns_version := installed_namespace_version_map.get(managed_ns_id):
                if ns_version != managed_ns_info["version"]:
                    _LOGGER.debug(
                        f"[_create_managed_namespace] update managed namespace: {managed_ns_id}"
                    )
                    namespace_vo = self.get_namespace(managed_ns_id, domain_id)
                    self.update_namespace_by_vo(managed_ns_info, namespace_vo)
            else:
                _LOGGER.debug(
                    f"[_create_managed_namespace] create new managed namespace: {managed_ns_id}"
                )
                self.create_namespace(managed_ns_info)

        return True
