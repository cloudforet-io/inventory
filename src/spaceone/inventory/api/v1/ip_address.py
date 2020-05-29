from spaceone.api.inventory.v1 import ip_address_pb2, ip_address_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class IPAddress(BaseAPI, ip_address_pb2_grpc.IPAddressServicer):

    pb2 = ip_address_pb2
    pb2_grpc = ip_address_pb2_grpc

    def allocate(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('IPService', metadata) as ip_service:
            return self.locator.get_info('IPInfo', ip_service.allocate(params))

    def reserve(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('IPService', metadata) as ip_service:
            return self.locator.get_info('IPInfo', ip_service.reserve(params))

    def release(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('IPService', metadata) as ip_service:
            ip_service.release(params)
            return self.locator.get_info('EmptyInfo')

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('IPService', metadata) as ip_service:
            return self.locator.get_info('IPInfo', ip_service.update(params))

    def pin_data(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('IPService', metadata) as ip_service:
            return self.locator.get_info('IPInfo', ip_service.pin_data(params))

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('IPService', metadata) as ip_service:
            return self.locator.get_info('IPInfo', ip_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('IPService', metadata) as ip_service:
            ip_vos, total_count = ip_service.list(params)
            return self.locator.get_info('IPsInfo', ip_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('IPService', metadata) as ip_service:
            return self.locator.get_info('StatisticsInfo', ip_service.stat(params))
