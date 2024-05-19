from typing import Tuple
from spaceone.core.error import *


class ResourceManager(object):
    resource_keys: list = None
    query_method = None

    """
    This is used by collector
    """

    def find_resources(self, query: dict) -> Tuple[list, int]:
        self._check_resource_finder_state()
        query["only"] = self.resource_keys

        resources = []
        vos, total_count = getattr(self, self.query_method)(query)

        for vo in vos:
            data = {}
            for key in self.resource_keys:
                data[key] = getattr(vo, key)

            resources.append(data)

        return resources, total_count

    def delete_resources(self, query: dict) -> int:
        self._check_resource_finder_state()
        query["only"] = self.resource_keys + ["updated_at"]

        vos, total_count = getattr(self, self.query_method)(query)
        vos.delete()

        return total_count

    def _check_resource_finder_state(self) -> None:
        if not (self.resource_keys and self.query_method):
            raise ERROR_UNKNOWN(message="ResourceManager is not set.")

        if getattr(self, self.query_method, None) is None:
            raise ERROR_UNKNOWN(message="ResourceManager is not set.")
