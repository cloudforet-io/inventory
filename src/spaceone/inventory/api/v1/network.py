from spaceone.api.inventory.v1 import network_pb2, network_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Network(BaseAPI, network_pb2_grpc.NetworkServicer):

    pb2 = network_pb2
    pb2_grpc = network_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkService', metadata) as network_service:
            return self.locator.get_info('NetworkInfo', network_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkService', metadata) as network_service:
            return self.locator.get_info('NetworkInfo', network_service.update(params))

    def pin_data(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkService', metadata) as network_service:
            return self.locator.get_info('NetworkInfo', network_service.pin_data(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkService', metadata) as network_service:
            network_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkService', metadata) as network_service:
            return self.locator.get_info('NetworkInfo', network_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkService', metadata) as network_service:
            network_vos, total_count = network_service.list(params)
            return self.locator.get_info('NetworksInfo', network_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkService', metadata) as network_service:
            return self.locator.get_info('StatisticsInfo', network_service.stat(params))
