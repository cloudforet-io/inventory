import logging
from typing import Union
from operator import itemgetter
from spaceone.core.manager import BaseManager
from spaceone.core import utils
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.manager.record_manager import RecordManager

_LOGGER = logging.getLogger(__name__)

DIFF_KEYS = [
    "name",
    "ip_addresses",
    "account",
    "instance_type",
    "instance_size",
    "reference",
    "region_code",
    "project_id",
    "data",
    "tags",
]

MAX_KEY_DEPTH = 3


class ChangeHistoryManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.record_mgr: RecordManager = self.locator.get_manager("RecordManager")
        self.merged_data = {}
        self.is_changed = False
        self.collector_id = self.transaction.get_meta("collector_id")
        self.job_id = self.transaction.get_meta("job_id")
        self.plugin_id = self.transaction.get_meta("plugin_id")
        self.secret_id = self.transaction.get_meta("secret.secret_id")
        self.service_account_id = self.transaction.get_meta("secret.service_account_id")
        self.user_id = self.transaction.get_meta("user_id")

        if (
            self.collector_id
            and self.job_id
            and self.service_account_id
            and self.plugin_id
        ):
            self.updated_by = "COLLECTOR"
        else:
            self.updated_by = "USER"

    def add_new_history(self, cloud_service_vo: CloudService, new_data: dict) -> None:
        self._create_record(cloud_service_vo, new_data)

    def add_update_history(
        self, cloud_service_vo: CloudService, new_data: dict, old_data: dict
    ) -> None:
        new_keys = new_data.keys()

        if len(set(new_keys) & set(DIFF_KEYS)) > 0:
            self._create_record(cloud_service_vo, new_data, old_data)

    def add_delete_history(self, cloud_service_vo: CloudService) -> None:
        params = {
            "cloud_service_id": cloud_service_vo.cloud_service_id,
            "domain_id": cloud_service_vo.domain_id,
            "action": "DELETE",
        }

        self.record_mgr.create_record(params)

    def _create_record(
        self, cloud_service_vo: CloudService, new_data: dict, old_data: dict = None
    ) -> None:
        if old_data:
            action = "UPDATE"
        else:
            action = "CREATE"

        metadata = new_data.get("metadata", {}).get(self.plugin_id or "MANUAL", {})
        exclude_keys = metadata.get("change_history", {}).get("exclude", [])

        diff = self._make_diff(new_data, old_data, exclude_keys)
        diff_count = len(diff)

        if diff_count > 0:
            params = {
                "cloud_service_id": cloud_service_vo.cloud_service_id,
                "domain_id": cloud_service_vo.domain_id,
                "action": action,
                "diff": diff,
                "diff_count": diff_count,
                "updated_by": self.updated_by,
            }

            if self.updated_by == "COLLECTOR":
                params["collector_id"] = self.collector_id
                params["job_id"] = self.job_id
            else:
                params["user_id"] = self.user_id

            self.record_mgr.create_record(params)

    def _make_diff(self, new_data: dict, old_data: dict, exclude_keys: list) -> list:
        diff = []
        for key in DIFF_KEYS:
            if key in new_data:
                if old_data:
                    old_value = old_data.get(key)
                else:
                    old_value = None

                diff += self._get_diff_data(key, new_data[key], old_value, exclude_keys)

        return diff

    def _get_diff_data(
        self,
        key: str,
        new_value: any,
        old_value: any,
        exclude_keys: list,
        depth: int = 1,
        parent_key: str = None,
    ) -> list:
        diff = []

        if depth == MAX_KEY_DEPTH:
            if new_value != old_value:
                diff_data = self._generate_diff_data(
                    key, parent_key, new_value, old_value, exclude_keys
                )
                if diff_data:
                    diff.append(diff_data)
        elif isinstance(new_value, dict):
            if parent_key:
                parent_key = f"{parent_key}.{key}"
            else:
                parent_key = key

            for sub_key, sub_value in new_value.items():
                if isinstance(old_value, dict):
                    sub_old_value = old_value.get(sub_key)
                else:
                    sub_old_value = None

                diff += self._get_diff_data(
                    sub_key,
                    sub_value,
                    sub_old_value,
                    exclude_keys,
                    depth + 1,
                    parent_key,
                )
        else:
            if new_value != old_value:
                diff_data = self._generate_diff_data(
                    key, parent_key, new_value, old_value, exclude_keys
                )
                if diff_data:
                    diff.append(diff_data)

        return diff

    def _generate_diff_data(
        self,
        key: str,
        parent_key: str,
        new_value: any,
        old_value: any,
        exclude_keys: list,
    ) -> Union[dict, None]:
        if old_value is None:
            diff_type = "ADDED"
        else:
            diff_type = "CHANGED"

        before = self._change_diff_value(old_value)
        after = self._change_diff_value(new_value)
        diff_key = key if parent_key is None else f"{parent_key}.{key}"

        if diff_key in exclude_keys:
            return None
        elif before == after:
            return None
        else:
            return {
                "key": diff_key,
                "before": before,
                "after": after,
                "type": diff_type,
            }

    def _change_diff_value(self, value: any) -> any:
        if isinstance(value, dict):
            return utils.dump_json(self._sort_dict_value(value))
        elif isinstance(value, list):
            return utils.dump_json(self._sort_list_values(value))
        elif value is None:
            return value
        else:
            return str(value)

    def _sort_dict_value(self, value: dict) -> dict:
        try:
            for k, v in value.items():
                if isinstance(v, dict):
                    value[k] = self._sort_dict_value(v)
                elif isinstance(v, list):
                    value[k] = self._sort_list_values(v)

            return dict(sorted(value.items()))
        except Exception as e:
            # _LOGGER.warning(
            #     f"[_sort_dict_value] dict value sort error: {e}", exc_info=True
            # )
            pass

        return value

    def _sort_list_values(self, values: list) -> list:
        if len(values) > 0:
            if isinstance(values[0], dict):
                changed_list_values = []
                for value in values:
                    changed_list_values.append(self._sort_dict_value(value))

                sort_keys = list(changed_list_values[0].keys())

                if len(sort_keys) > 0:
                    try:
                        return sorted(
                            changed_list_values, key=itemgetter(*sort_keys[:3])
                        )
                    except Exception as e:
                        # _LOGGER.warning(
                        #     f"[_sort_list_values] list value sort error: {e}",
                        #     exc_info=True,
                        # )
                        pass

                return changed_list_values

            else:
                return sorted(values)

        return values
