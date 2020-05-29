from spaceone.api.inventory.v1 import server_pb2, server_pb2_grpc
from spaceone.core.pygrpc import BaseAPI
from spaceone.inventory.service.server_service import ServerService


class Server(BaseAPI, server_pb2_grpc.ServerServicer):

    pb2 = server_pb2
    pb2_grpc = server_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ServerService', metadata) as server_service:
            return self.locator.get_info('ServerInfo', server_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ServerService', metadata) as server_service:
            return self.locator.get_info('ServerInfo', server_service.update(params))

    def pin_data(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ServerService', metadata) as server_service:
            return self.locator.get_info('ServerInfo', server_service.pin_data(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ServerService', metadata) as server_service:
            server_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ServerService', metadata) as server_service:
            return self.locator.get_info('ServerInfo', server_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ServerService', metadata) as server_service:
            server_vos, total_count = server_service.list(params)
            return self.locator.get_info('ServersInfo', server_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ServerService', metadata) as server_service:
            return self.locator.get_info('StatisticsInfo', server_service.stat(params))
