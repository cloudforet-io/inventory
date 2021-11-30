from spaceone.api.inventory.v1 import cloud_service_pb2, cloud_service_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class CloudService(BaseAPI, cloud_service_pb2_grpc.CloudServiceServicer):

    pb2 = cloud_service_pb2
    pb2_grpc = cloud_service_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceService', metadata) as cloud_svc_service:
            return self.locator.get_info('CloudServiceInfo', cloud_svc_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceService', metadata) as cloud_svc_service:
            return self.locator.get_info('CloudServiceInfo', cloud_svc_service.update(params))

    def pin_data(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceService', metadata) as cloud_svc_service:
            return self.locator.get_info('CloudServiceInfo', cloud_svc_service.pin_data(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceService', metadata) as cloud_svc_service:
            cloud_svc_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceService', metadata) as cloud_svc_service:
            return self.locator.get_info('CloudServiceInfo', cloud_svc_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceService', metadata) as cloud_svc_service:
            cloud_svc_vos, total_count = cloud_svc_service.list(params)
            return self.locator.get_info('CloudServicesInfo',
                                         cloud_svc_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceService', metadata) as cloud_svc_service:
            return self.locator.get_info('StatisticsInfo', cloud_svc_service.stat(params))
