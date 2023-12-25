import copy
import logging
from typing import Tuple

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.lib.resource_manager import ResourceManager
from spaceone.inventory.manager.cloud_service_query_set_manager import (
    CloudServiceQuerySetManager,
)


_LOGGER = logging.getLogger(__name__)


class CloudServiceTypeManager(BaseManager, ResourceManager):
    resource_keys = ["cloud_service_type_id"]
    query_method = "list_cloud_service_types"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_type_model: CloudServiceType = self.locator.get_model(
            "CloudServiceType"
        )
        self.cloud_svc_query_set_mgr: CloudServiceQuerySetManager = (
            self.locator.get_manager("CloudServiceQuerySetManager")
        )

    def create_cloud_service_type(self, params: dict) -> CloudServiceType:
        def _rollback(vo: CloudServiceType):
            _LOGGER.info(
                f"[ROLLBACK] Delete Cloud Service Type : {vo.name} ({vo.cloud_service_type_id})"
            )
            vo.delete()

        cloud_svc_type_vo: CloudServiceType = self.cloud_svc_type_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_type_vo)

        self._create_cloud_service_query_sets(
            params.get("metadata", {}), cloud_svc_type_vo
        )

        return cloud_svc_type_vo

    def update_cloud_service_type_by_vo(
        self, params: dict, cloud_svc_type_vo: CloudServiceType
    ):
        def _rollback(old_data):
            _LOGGER.info(
                f'[ROLLBACK] Revert Data : {old_data.get("cloud_service_type_id")}'
            )
            cloud_svc_type_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cloud_svc_type_vo.to_dict())

        self._update_cloud_service_query_sets(
            params.get("metadata", {}), cloud_svc_type_vo
        )

        return cloud_svc_type_vo.update(params)

    def delete_cloud_service_type_by_vo(self, cloud_svc_type_vo) -> None:
        self._delete_cloud_service_query_sets(cloud_svc_type_vo)
        cloud_svc_type_vo.delete()

    def get_cloud_service_type(
        self,
        cloud_service_type_id: str,
        domain_id: str,
        workspace_id: str = None,
    ) -> CloudServiceType:
        conditions = {
            "cloud_service_type_id": cloud_service_type_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions.update({"workspace_id": workspace_id})

        return self.cloud_svc_type_model.get(**conditions)

    def filter_cloud_service_types(self, **conditions) -> QuerySet:
        return self.cloud_svc_type_model.filter(**conditions)

    def list_cloud_service_types(self, query: dict) -> Tuple[QuerySet, int]:
        cloud_svc_type_vos, total_count = self.cloud_svc_type_model.query(**query)
        return cloud_svc_type_vos, total_count

    def stat_cloud_service_types(self, query: dict) -> dict:
        return self.cloud_svc_type_model.stat(**query)

    def _create_cloud_service_query_sets(
        self, metadata: dict, cloud_service_type_vo: CloudServiceType
    ) -> None:
        for query_set in metadata.get("query_sets", []):
            if "name" in query_set:
                create_params = copy.deepcopy(query_set)
                self._create_cloud_service_query_set(
                    create_params, cloud_service_type_vo
                )

    def _update_cloud_service_query_sets(
        self, metadata: dict, cloud_service_type_vo: CloudServiceType
    ) -> None:
        query_set_vos = self._filter_cloud_service_query_sets(cloud_service_type_vo)
        query_set_info = {}

        for query_set_vo in query_set_vos:
            query_set_info[query_set_vo.name] = query_set_vo.query_set_id

        for query_set in metadata.get("query_sets", []):
            if name := query_set.get("name"):
                if name in query_set_info:
                    update_params = copy.deepcopy(query_set)
                    query_options = update_params.get("query_options", {})
                    if query_set_vos[0].query_hash != utils.dict_to_hash(query_options):
                        self.cloud_svc_query_set_mgr.update_cloud_service_query_set_by_vo(
                            update_params, query_set_vos[0]
                        )
                else:
                    create_params = copy.deepcopy(query_set)
                    self._create_cloud_service_query_set(
                        create_params, cloud_service_type_vo
                    )

                del query_set_info[name]

        for query_set_id in query_set_info.values():
            query_set_vo = self.cloud_svc_query_set_mgr.get_cloud_service_query_set(
                query_set_id, cloud_service_type_vo.domain_id
            )
            self.cloud_svc_query_set_mgr.delete_cloud_service_query_set_by_vo(
                query_set_vo
            )

    def _delete_cloud_service_query_sets(
        self, cloud_service_type_vo: CloudServiceType
    ) -> None:
        query_set_vos = self._filter_cloud_service_query_sets(cloud_service_type_vo)
        for query_set_vo in query_set_vos:
            self.cloud_svc_query_set_mgr.delete_cloud_service_query_set_by_vo(
                query_set_vo
            )

    def _create_cloud_service_query_set(
        self, create_params: dict, cloud_service_type_vo: CloudServiceType
    ) -> None:
        create_params["query_type"] = "MANAGED"
        create_params["provider"] = cloud_service_type_vo.provider
        create_params["cloud_service_group"] = cloud_service_type_vo.group
        create_params["cloud_service_type"] = cloud_service_type_vo.name
        create_params["workspace_id"] = cloud_service_type_vo.workspace_id
        create_params["domain_id"] = cloud_service_type_vo.domain_id
        self.cloud_svc_query_set_mgr.create_cloud_service_query_set(create_params)

    def _filter_cloud_service_query_sets(
        self, cloud_service_type_vo: CloudServiceType
    ) -> QuerySet:
        filter_params = {
            "provider": cloud_service_type_vo.provider,
            "cloud_service_group": cloud_service_type_vo.group,
            "cloud_service_type": cloud_service_type_vo.name,
            "workspace_id": cloud_service_type_vo.workspace_id,
            "domain_id": cloud_service_type_vo.domain_id,
        }

        return self.cloud_svc_query_set_mgr.filter_cloud_service_query_sets(
            **filter_params
        )
