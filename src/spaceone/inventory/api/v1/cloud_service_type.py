from spaceone.api.inventory.v1 import cloud_service_type_pb2, cloud_service_type_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class CloudServiceType(BaseAPI, cloud_service_type_pb2_grpc.CloudServiceTypeServicer):

    pb2 = cloud_service_type_pb2
    pb2_grpc = cloud_service_type_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceTypeService', metadata) as cloud_svc_type_service:
            return self.locator.get_info('CloudServiceTypeInfo', cloud_svc_type_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceTypeService', metadata) as cloud_svc_type_service:
            return self.locator.get_info('CloudServiceTypeInfo', cloud_svc_type_service.update(params))

    def pin_data(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceTypeService', metadata) as cloud_svc_type_service:
            return self.locator.get_info('CloudServiceTypeInfo', cloud_svc_type_service.pin_data(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceTypeService', metadata) as cloud_svc_type_service:
            cloud_svc_type_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceTypeService', metadata) as cloud_svc_type_service:
            return self.locator.get_info('CloudServiceTypeInfo', cloud_svc_type_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceTypeService', metadata) as cloud_svc_type_service:
            cloud_svc_type_vos, total_count = cloud_svc_type_service.list(params)
            return self.locator.get_info('CloudServiceTypesInfo',
                                         cloud_svc_type_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceTypeService', metadata) as cloud_svc_type_service:
            return self.locator.get_info('StatisticsInfo', cloud_svc_type_service.stat(params))
