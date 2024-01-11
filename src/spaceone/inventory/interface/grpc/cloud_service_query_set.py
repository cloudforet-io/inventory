from spaceone.api.inventory.v1 import (
    cloud_service_query_set_pb2,
    cloud_service_query_set_pb2_grpc,
)
from spaceone.core.pygrpc import BaseAPI


class CloudServiceQuerySet(
    BaseAPI, cloud_service_query_set_pb2_grpc.CloudServiceQuerySetServicer
):
    pb2 = cloud_service_query_set_pb2
    pb2_grpc = cloud_service_query_set_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            response = self.locator.get_info(
                "CloudServiceQuerySetInfo", cloud_svc_query_set_service.create(params)
            )
            return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            response = self.locator.get_info(
                "CloudServiceQuerySetInfo", cloud_svc_query_set_service.update(params)
            )
            return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            cloud_svc_query_set_service.delete(params)
            return self.empty()

    def run(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            cloud_svc_query_set_service.run(params)
            return self.empty()

    def test(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            response = cloud_svc_query_set_service.test(params)
            return self.dict_to_message(response)

    def enable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            response = self.locator.get_info(
                "CloudServiceQuerySetInfo", cloud_svc_query_set_service.enable(params)
            )
            return self.dict_to_message(response)

    def disable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            response = self.locator.get_info(
                "CloudServiceQuerySetInfo", cloud_svc_query_set_service.disable(params)
            )
            return self.dict_to_message(response)

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            response = self.locator.get_info(
                "CloudServiceQuerySetInfo", cloud_svc_query_set_service.get(params)
            )
            return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            query_set_vos, total_count = cloud_svc_query_set_service.list(params)
            response = self.locator.get_info(
                "CloudServiceQuerySetsInfo",
                query_set_vos,
                total_count,
                minimal=self.get_minimal(params),
            )
            return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceQuerySetService", metadata
        ) as cloud_svc_query_set_service:
            response = cloud_svc_query_set_service.stat(params)
            return self.dict_to_message(response)
