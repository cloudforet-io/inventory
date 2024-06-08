from spaceone.core.pygrpc import BaseAPI
from spaceone.api.inventory.plugin import collector_pb2, collector_pb2_grpc
from spaceone.inventory.plugin.collector.error.response import *
from spaceone.inventory.plugin.collector.service.collector_service import (
    CollectorService,
)

RESOURCE_TYPE_MAP = {
    "inventory.CloudServiceType": "cloud_service_type",
    "inventory.CloudService": "cloud_service",
    "inventory.ErrorResource": "error_data",
    "inventory.Region": "region",
    "inventory.Namespace": "namespace",
    "inventory.Metric": "metric",
}


class Collector(BaseAPI, collector_pb2_grpc.CollectorServicer):
    pb2 = collector_pb2
    pb2_grpc = collector_pb2_grpc

    def init(self, request, context):
        params, metadata = self.parse_request(request, context)
        collector_svc = CollectorService(metadata)
        response: dict = collector_svc.init(params)
        return self.dict_to_message(response)

    def verify(self, request, context):
        params, metadata = self.parse_request(request, context)
        collector_svc = CollectorService(metadata)
        collector_svc.verify(params)
        return self.empty()

    def collect(self, request, context):
        params, metadata = self.parse_request(request, context)
        collector_svc = CollectorService(metadata)
        for response in collector_svc.collect(params):
            response = self._parse_response_by_resource_type(response)

            if response["state"] == "FAILURE":
                response["resource_type"] = "inventory.ErrorResource"

            yield self.dict_to_message(response)

    def _parse_response_by_resource_type(self, response: dict) -> dict:
        if resource_type := response.get("resource_type"):
            if resource_type in RESOURCE_TYPE_MAP:
                resource_field = RESOURCE_TYPE_MAP[resource_type]
                return {
                    "resource_type": resource_type,
                    "state": response.get("state", "SUCCESS"),
                    "match_rules": self._make_match_rules(
                        response.get("match_keys", [])
                    ),
                    "resource": response[resource_field],
                    "message": response.get("error_message"),
                }
            else:
                raise ERROR_NOT_SUPPORTED_RESOURCE_TYPE(resource_type=resource_type)

    @staticmethod
    def _make_match_rules(match_keys: list = None) -> dict:
        if match_keys:
            match_rules = {}
            for idx, keys in enumerate(match_keys, 1):
                match_rules[str(idx)] = keys

            return match_rules
        else:
            return {}
