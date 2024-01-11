from spaceone.api.inventory.v1 import (
    cloud_service_report_pb2,
    cloud_service_report_pb2_grpc,
)
from spaceone.core.pygrpc import BaseAPI


class CloudServiceReport(
    BaseAPI, cloud_service_report_pb2_grpc.CloudServiceReportServicer
):
    pb2 = cloud_service_report_pb2
    pb2_grpc = cloud_service_report_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceReportService", metadata
        ) as cloud_svc_report_service:
            response = self.locator.get_info(
                "CloudServiceReportInfo", cloud_svc_report_service.create(params)
            )
            return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceReportService", metadata
        ) as cloud_svc_report_service:
            response = self.locator.get_info(
                "CloudServiceReportInfo", cloud_svc_report_service.update(params)
            )
            return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceReportService", metadata
        ) as cloud_svc_report_service:
            cloud_svc_report_service.delete(params)
            return self.empty()

    def send(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceReportService", metadata
        ) as cloud_svc_report_service:
            cloud_svc_report_service.send(params)
            return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceReportService", metadata
        ) as cloud_svc_report_service:
            response = self.locator.get_info(
                "CloudServiceReportInfo", cloud_svc_report_service.get(params)
            )
            return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceReportService", metadata
        ) as cloud_svc_report_service:
            cloud_svc_type_vos, total_count = cloud_svc_report_service.list(params)
            response = self.locator.get_info(
                "CloudServiceReportsInfo",
                cloud_svc_type_vos,
                total_count,
                minimal=self.get_minimal(params),
            )
            return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service(
            "CloudServiceReportService", metadata
        ) as cloud_svc_report_service:
            response = cloud_svc_report_service.stat(params)
            return self.dict_to_message(response)
