from spaceone.core.pygrpc import BaseAPI
from spaceone.api.inventory.plugin import collector_pb2, collector_pb2_grpc
from spaceone.inventory.plugin.collector.error.response import *
from spaceone.inventory.plugin.collector.service.collector_service import (
    CollectorService,
)

VALID_RESOURCE_TYPES = {
    "cloud_service_type": "inventory.CloudServiceType",
    "cloud_service": "inventory.CloudService",
    "error_data": "inventory.ErrorResource",
    "region": "inventory.Region",
    "namespace": "inventory.Namespace",
    "metric": "inventory.Metric",
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
            response = self._select_valid_resource_and_resource_type(response)

            if "cloud_service_type" in response:
                cloud_service_type = response.pop("cloud_service_type")
                response["resource"] = cloud_service_type

            if "cloud_service" in response:
                cloud_service = response.pop("cloud_service")
                response["resource"] = cloud_service

            if "error_data" in response:
                error_data = response.pop("error_data")
                response["resource"] = error_data

            if "region" in response:
                region = response.pop("region")
                response["resource"] = region

            if "namespace" in response:
                namespace = response.pop("namespace")
                response["resource"] = namespace

            if "metric" in response:
                metric = response.pop("metric")
                response["resource"] = metric

            if response["state"] == "FAILURE":
                response["resource_type"] = "inventory.ErrorResource"

            if "error_message" in response:
                error_message = response.pop("error_message")
                response["message"] = error_message

            if "match_keys" in response:
                match_keys = response.pop("match_keys")
                response["match_rules"] = {}

                for idx, keys in enumerate(match_keys, 1):
                    response["match_rules"][str(idx)] = keys
            yield self.dict_to_message(response)

    def _select_valid_resource_and_resource_type(self, response: dict) -> dict:
        resources = list(VALID_RESOURCE_TYPES.keys())
        valid_resource = [
            key for key in response.keys() if key in resources and response[key]
        ]

        self._check_resource_and_resource_type(valid_resource, response)

        resources.remove(valid_resource[0])
        for key in resources:
            del response[key]

        return response

    @staticmethod
    def _check_resource_and_resource_type(valid_resource, response) -> None:
        if not len(valid_resource):
            raise ERROR_NO_INPUT_FIELD()

        if len(valid_resource) != 1:
            raise ERROR_INVAILD_INPUT_FIELD(fields=valid_resource)

        resource_type = response["resource_type"]
        if resource_type != VALID_RESOURCE_TYPES[valid_resource[0]]:
            raise ERROR_NOT_MATCH_RESOURCE_TYPE(
                resource_type=resource_type, resource=valid_resource[0]
            )
