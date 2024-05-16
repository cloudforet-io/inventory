import logging
import copy
import math
import pytz
from typing import Tuple, List
from datetime import datetime

from spaceone.core.model.mongo_model import QuerySet
from spaceone.core.manager import BaseManager
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.lib.resource_manager import ResourceManager
from spaceone.inventory.manager.collection_state_manager import CollectionStateManager
from spaceone.inventory.manager.reference_manager import ReferenceManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)

MERGE_KEYS = [
    "name",
    "ip_addresses",
    "account",
    "instance_type",
    "instance_size",
    "reference" "region_code",
    "ref_region",
    "project_id",
    "data",
]

SIZE_MAP = {
    "KB": 1024,
    "MB": 1024 * 1024,
    "GB": 1024 * 1024 * 1024,
    "TB": 1024 * 1024 * 1024 * 1024,
    "PB": 1024 * 1024 * 1024 * 1024 * 1024,
    "EB": 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
    "ZB": 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
    "YB": 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
}


class CloudServiceManager(BaseManager, ResourceManager):
    resource_keys = ["cloud_service_id"]
    query_method = "list_cloud_services"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_model: CloudService = self.locator.get_model("CloudService")

    def create_cloud_service(self, params: dict) -> CloudService:
        def _rollback(vo: CloudService):
            _LOGGER.info(
                f"[ROLLBACK] Delete Cloud Service : {vo.provider} ({vo.cloud_service_type})"
            )
            vo.terminate()

        cloud_svc_vo: CloudService = self.cloud_svc_model.create(params)
        self.transaction.add_rollback(_rollback, cloud_svc_vo)

        return cloud_svc_vo

    def update_cloud_service_by_vo(
        self, params: dict, cloud_svc_vo: CloudService
    ) -> CloudService:
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Data : {old_data.get("cloud_service_id")}')
            cloud_svc_vo.update(old_data)

        self.transaction.add_rollback(_rollback, cloud_svc_vo.to_dict())
        cloud_svc_vo: CloudService = cloud_svc_vo.update(params)

        return cloud_svc_vo

    @staticmethod
    def delete_cloud_service_by_vo(cloud_svc_vo: CloudService) -> None:
        cloud_svc_vo.delete()

    def terminate_cloud_service(
        self, cloud_service_id: str, domain_id: str, workspace_id: str = None
    ) -> None:
        cloud_svc_vo: CloudService = self.get_cloud_service(
            cloud_service_id, domain_id, workspace_id
        )
        cloud_svc_vo.terminate()

    def get_cloud_service(
        self,
        cloud_service_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects: list = None,
    ):
        conditions = {"cloud_service_id": cloud_service_id, "domain_id": domain_id}

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        return self.cloud_svc_model.get(**conditions)

    def filter_cloud_services(self, **conditions) -> QuerySet:
        return self.cloud_svc_model.filter(**conditions)

    def list_cloud_services(
        self,
        query: dict,
        target: str = None,
        change_filter: bool = False,
        domain_id: str = None,
        reference_filter: dict = None,
    ) -> Tuple[QuerySet, int]:
        if change_filter:
            query = self._change_filter_tags(query)
            query = self._change_only_tags(query)
            query = self._change_sort_tags(query)
            query = self._change_filter_project_group_id(query, domain_id)

            # Append Query for DELETED filter (Temporary Logic)
            query = self._append_state_query(query)

        return self.cloud_svc_model.query(
            **query, target=target, reference_filter=reference_filter
        )

    def analyze_cloud_services(
        self,
        query: dict,
        change_filter: bool = False,
        domain_id: str = None,
        reference_filter: dict = None,
    ):
        if change_filter:
            query = self._change_filter_tags(query)
            query = self._change_filter_project_group_id(query, domain_id)

            # Append Query for DELETED filter (Temporary Logic)
            query = self._append_state_query(query)

        return self.cloud_svc_model.analyze(**query, reference_filter=reference_filter)

    def stat_cloud_services(
        self,
        query: dict,
        change_filter: bool = False,
        domain_id: str = None,
        reference_filter: dict = None,
    ):
        if change_filter:
            query = self._change_filter_tags(query)
            query = self._change_distinct_tags(query)
            query = self._change_filter_project_group_id(query, domain_id)

            # Append Query for DELETED filter (Temporary Logic)
            query = self._append_state_query(query)

        return self.cloud_svc_model.stat(**query, reference_filter=reference_filter)

    def get_export_query_results(
        self,
        options: list,
        timezone: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects: list = None,
    ):
        ref_mgr: ReferenceManager = self.locator.get_manager(ReferenceManager)

        for export_option in options:
            self._check_export_option(export_option)

            if export_option["query_type"] == "SEARCH":
                export_option["search_query"] = self._change_export_query(
                    "SEARCH",
                    export_option["search_query"],
                    domain_id,
                    workspace_id,
                    user_projects,
                )
                export_option["results"] = self._get_search_query_results(
                    export_option["search_query"],
                    timezone,
                    domain_id,
                    workspace_id,
                    ref_mgr,
                )
            else:
                export_option["analyze_query"] = self._change_export_query(
                    "ANALYZE",
                    export_option["analyze_query"],
                    domain_id,
                    workspace_id,
                    user_projects,
                )
                export_option["results"] = self._get_analyze_query_results(
                    export_option["analyze_query"], domain_id
                )

        return options

    @staticmethod
    def _convert_size(value: any = None, source_unit: str = None) -> any:
        if value is None:
            value = 0

        if isinstance(value, float) or isinstance(value, int):
            value = value * SIZE_MAP.get(source_unit, 1)

            if value == 0:
                return "0 B"
            size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
            i = int(math.floor(math.log(value, 1024)))
            p = math.pow(1024, i)
            s = round(value / p, 2)

            if math.ceil(s) == math.floor(s):
                s = int(s)

            return "%s %s" % (s, size_name[i])
        else:
            return value

    def _convert_data(
        self,
        value: any,
        data_type: str,
        prefix: str = None,
        postfix: str = None,
        default: any = None,
        source_unit: str = "BYTES",
    ) -> any:
        if isinstance(value, float):
            if math.ceil(value) == math.floor(value):
                value = int(value)
        elif isinstance(value, bool):
            value = str(value)
        elif isinstance(value, list):
            values = []
            for v in value:
                converted_value = self._convert_data(
                    v, data_type, prefix, postfix, default, source_unit
                )
                if converted_value is not None and str(converted_value).strip() != "":
                    values.append(str(converted_value))

            return "\n".join(values)

        if value is None or str(value).strip() == "":
            value = default

        if data_type == "size":
            value = self._convert_size(value, source_unit)

        if prefix:
            value = f"{prefix}{value}"

        if postfix:
            value = f"{value}{postfix}"

        return value

    def _get_search_query_results(
        self,
        query: dict,
        timezone: str,
        domain_id: str,
        workspace_id: str,
        ref_mgr: ReferenceManager,
    ):
        cloud_service_vos, total_count = self.list_cloud_services(
            query, change_filter=True, domain_id=domain_id
        )
        results = []

        fields = query.get("fields")
        if fields is None:
            raise ERROR_REQUIRED_PARAMETER(key="options[].search_query.fields")

        tz = pytz.timezone(timezone)
        tz_offset = tz.utcoffset(datetime.utcnow())

        for cloud_service_vo in cloud_service_vos:
            cloud_service_data = cloud_service_vo.to_dict()

            result = {}
            for field in fields:
                if isinstance(field, dict):
                    key = field["key"]
                    name = field.get("name") or key
                    reference = field.get("reference", {})
                    data_type = field.get("type", "text")
                    options = field.get("options", {})
                    prefix = options.get("prefix")
                    postfix = options.get("postfix")
                    default = options.get("default")
                    source_unit = options.get("source_unit")

                    if key.startswith("tags."):
                        key = self._get_hashed_key(key)

                    value = utils.get_dict_value(cloud_service_data, key)

                    if resource_type := reference.get("resource_type"):
                        if isinstance(value, list):
                            value = [
                                ref_mgr.get_reference_name(
                                    resource_type, v, domain_id, workspace_id
                                )
                                for v in value
                            ]
                        else:
                            value = ref_mgr.get_reference_name(
                                resource_type, value, domain_id, workspace_id
                            )

                else:
                    key = field
                    name = field
                    data_type = "text"
                    prefix = None
                    postfix = None
                    default = None
                    source_unit = None

                    if key.startswith("tags."):
                        key = self._get_hashed_key(key)

                    value = utils.get_dict_value(cloud_service_data, key)

                if key in ["created_at", "updated_at", "deleted_at"] and isinstance(
                    value, datetime
                ):
                    value = value + tz_offset

                result[name] = self._convert_data(
                    value,
                    data_type,
                    prefix=prefix,
                    postfix=postfix,
                    default=default,
                    source_unit=source_unit,
                )

            results.append(result)

        return results

    def _get_analyze_query_results(self, query: dict, domain_id: str) -> List[dict]:
        response = self.analyze_cloud_services(
            query, change_filter=True, domain_id=domain_id
        )
        return response.get("results", [])

    @staticmethod
    def merge_data(new_data: dict, old_data: dict) -> dict:
        for key in MERGE_KEYS:
            if key in new_data:
                new_value = new_data[key]
                old_value = old_data.get(key)
                if key in ["data", "tags"]:
                    is_changed = False
                    for sub_key, sub_value in new_value.items():
                        if sub_value != old_value.get(sub_key):
                            is_changed = True
                            break

                    if is_changed:
                        merged_value = copy.deepcopy(old_value)
                        merged_value.update(new_value)
                        new_data[key] = merged_value
                    else:
                        del new_data[key]
                else:
                    if new_value == old_value:
                        del new_data[key]

        return new_data

    def find_resources(self, query: dict) -> Tuple[List[dict], int]:
        query["only"] = ["cloud_service_id"]
        query["filter"].append({"k": "state", "v": "DELETED", "o": "not"})

        resources = []
        cloud_svc_vos, total_count = self.list_cloud_services(
            query, target="SECONDARY_PREFERRED"
        )

        for cloud_svc_vo in cloud_svc_vos:
            resources.append({"cloud_service_id": cloud_svc_vo.cloud_service_id})

        return resources, total_count

    def delete_resources(self, query: dict) -> int:
        query["only"] = self.resource_keys
        query["filter"].append({"k": "state", "v": "DELETED", "o": "not"})

        vos, total_count = self.list_cloud_services(query)

        cloud_service_ids = []
        for vo in vos:
            cloud_service_ids.append(vo.cloud_service_id)

        vos.update({"state": "DELETED", "deleted_at": datetime.utcnow()})

        state_mgr: CollectionStateManager = self.locator.get_manager(
            "CollectionStateManager"
        )
        state_mgr.delete_collection_state_by_cloud_service_ids(cloud_service_ids)

        return total_count

    @staticmethod
    def _append_state_query(query: dict) -> dict:
        state_default_filter = {"key": "state", "value": "ACTIVE", "operator": "eq"}

        show_deleted_resource = False
        for condition in query.get("filter", []):
            key = condition.get("k", condition.get("key"))
            value = condition.get("v", condition.get("value"))
            operator = condition.get("o", condition.get("operator"))

            if key == "state":
                if operator == "eq" and value == "DELETED":
                    show_deleted_resource = True
                elif operator in ["in", "contain_in"] and "DELETED" in value:
                    show_deleted_resource = True

        if not show_deleted_resource:
            query["filter"] = query.get("filter", [])
            query["filter"].append(state_default_filter)

        return query

    def _change_filter_project_group_id(self, query: dict, domain_id: str) -> dict:
        change_filter = []
        self.identity_mgr = None

        for condition in query.get("filter", []):
            key = condition.get("k", condition.get("key"))
            value = condition.get("v", condition.get("value"))
            operator = condition.get("o", condition.get("operator"))

            if key == "project_group_id":
                if self.identity_mgr is None:
                    self.identity_mgr: IdentityManager = self.locator.get_manager(
                        "IdentityManager"
                    )

                project_groups_info = self.identity_mgr.list_project_groups(
                    {
                        "query": {
                            "only": ["project_group_id"],
                            "filter": [{"k": key, "v": value, "o": operator}],
                        }
                    },
                    domain_id,
                )

                project_group_ids = [
                    project_group_info["project_group_id"]
                    for project_group_info in project_groups_info.get("results", [])
                ]

                project_ids = []

                for project_group_id in project_group_ids:
                    projects_info = self.identity_mgr.get_projects_in_project_group(
                        project_group_id
                    )
                    project_ids.extend(
                        [
                            project_info["project_id"]
                            for project_info in projects_info.get("results", [])
                        ]
                    )

                project_ids = list(set(project_ids))
                change_filter.append({"k": "project_id", "v": project_ids, "o": "in"})

            else:
                change_filter.append(condition)

        query["filter"] = change_filter
        return query

    def _change_filter_tags(self, query: dict) -> dict:
        change_filter = []

        for condition in query.get("filter", []):
            key = condition.get("k", condition.get("key"))
            value = condition.get("v", condition.get("value"))
            operator = condition.get("o", condition.get("operator"))

            if key.startswith("tags."):
                hashed_key = self._get_hashed_key(key)

                change_filter.append(
                    {"key": hashed_key, "value": value, "operator": operator}
                )

            else:
                change_filter.append(condition)

        query["filter"] = change_filter
        return query

    def _change_only_tags(self, query: dict) -> dict:
        change_only_tags = []
        if "only" in query:
            for key in query.get("only", []):
                if key.startswith("tags."):
                    hashed_key = self._get_hashed_key(key, only=True)
                    change_only_tags.append(hashed_key)
                else:
                    change_only_tags.append(key)
            query["only"] = change_only_tags

        return query

    def _change_distinct_tags(self, query: dict) -> dict:
        if "distinct" in query:
            distinct_key = query["distinct"]
            if distinct_key.startswith("tags."):
                hashed_key = self._get_hashed_key(distinct_key)
                query["distinct"] = hashed_key

        return query

    def _change_sort_tags(self, query: dict) -> dict:
        if sort_conditions := query.get("sort"):
            change_filter = []
            for condition in sort_conditions:
                sort_key = condition.get("key", "")
                desc = condition.get("desc", False)

                if sort_key.startswith("tags."):
                    hashed_key = self._get_hashed_key(sort_key)
                    change_filter.append({"key": hashed_key, "desc": desc})
                else:
                    change_filter.append({"key": sort_key, "desc": desc})

            query["sort"] = change_filter

        return query

    @staticmethod
    def _get_hashed_key(key: str, only: bool = False) -> str:
        if key.count(".") < 2:
            return key

        prefix, provider, key = key.split(".", 2)
        hash_key = utils.string_to_hash(key)
        if only:
            return f"{prefix}.{provider}.{hash_key}"
        else:
            return f"{prefix}.{provider}.{hash_key}.value"

    @staticmethod
    def _check_export_option(export_option: dict) -> None:
        if "name" not in export_option:
            raise ERROR_REQUIRED_PARAMETER(key="options[].name")

        query_type = export_option.get("query_type")

        if query_type == "SEARCH":
            if "search_query" not in export_option:
                raise ERROR_REQUIRED_PARAMETER(key="options[].search_query")
        elif query_type == "ANALYZE":
            if "analyze_query" not in export_option:
                raise ERROR_REQUIRED_PARAMETER(key="options[].analyze_query")
        else:
            raise ERROR_REQUIRED_PARAMETER(key="options[].query_type")

    @staticmethod
    def _change_export_query(
        query_type: str,
        query: dict,
        domain_id: str,
        workspace_id: str = None,
        user_projects: list = None,
    ):
        query["filter"] = query.get("filter", [])
        query["filter_or"] = query.get("filter_or", [])
        keyword = query.get("keyword")

        query["filter"].append({"k": "domain_id", "v": domain_id, "o": "eq"})

        if workspace_id:
            query["filter"].append({"k": "workspace_id", "v": workspace_id, "o": "eq"})

        if user_projects:
            query["filter"].append(
                {"k": "user_projects", "v": user_projects, "o": "in"}
            )

        if keyword:
            keyword = keyword.strip()
            if len(keyword) > 0:
                for key in [
                    "cloud_service_id",
                    "name",
                    "ip_addresses",
                    "cloud_service_group",
                    "cloud_service_type",
                    "reference.resource_id",
                ]:
                    query["filter_or"].append(
                        {
                            "k": key,
                            "v": list(filter(None, keyword.split(" "))),
                            "o": "contain_in",
                        }
                    )

            del query["keyword"]

        if query_type == "SEARCH":
            query["only"] = []
            fields = query.get("fields", [])
            for field in fields:
                if isinstance(field, dict):
                    if key := field.get("key"):
                        query["only"].append(key)
                    else:
                        raise ERROR_REQUIRED_PARAMETER(
                            key="options[].search_query.fields.key"
                        )
                elif isinstance(field, str):
                    query["only"].append(field)
                else:
                    raise ERROR_INVALID_PARAMETER_TYPE(
                        key="options[].search_query.fields", type="str or dict"
                    )

        return query
