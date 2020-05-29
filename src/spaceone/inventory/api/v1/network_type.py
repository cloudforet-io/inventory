from spaceone.api.inventory.v1 import network_type_pb2, network_type_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class NetworkType(BaseAPI, network_type_pb2_grpc.NetworkTypeServicer):

    pb2 = network_type_pb2
    pb2_grpc = network_type_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkTypeService', metadata) as ntype_service:
            return self.locator.get_info('NetworkTypeInfo', ntype_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkTypeService', metadata) as ntype_service:
            return self.locator.get_info('NetworkTypeInfo', ntype_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkTypeService', metadata) as ntype_service:
            ntype_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkTypeService', metadata) as ntype_service:
            return self.locator.get_info('NetworkTypeInfo', ntype_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkTypeService', metadata) as ntype_service:
            ntype_vos, total_count = ntype_service.list(params)
            return self.locator.get_info('NetworkTypesInfo', ntype_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkTypeService', metadata) as ntype_service:
            return self.locator.get_info('StatisticsInfo', ntype_service.stat(params))
