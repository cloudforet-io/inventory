from spaceone.api.inventory.v1 import subnet_pb2, subnet_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Subnet(BaseAPI, subnet_pb2_grpc.SubnetServicer):

    pb2 = subnet_pb2
    pb2_grpc = subnet_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('SubnetService', metadata) as subnet_service:
            return self.locator.get_info('SubnetInfo', subnet_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('SubnetService', metadata) as subnet_service:
            return self.locator.get_info('SubnetInfo', subnet_service.update(params))

    def pin_data(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('SubnetService', metadata) as subnet_service:
            return self.locator.get_info('SubnetInfo', subnet_service.pin_data(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('SubnetService', metadata) as subnet_service:
            subnet_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('SubnetService', metadata) as subnet_service:
            return self.locator.get_info('SubnetInfo', subnet_service.get(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('SubnetService', metadata) as subnet_service:
            return self.locator.get_info('StatisticsInfo', subnet_service.stat(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('SubnetService', metadata) as subnet_service:
            subnet_vos, total_count = subnet_service.list(params)
            return self.locator.get_info('SubnetsInfo', subnet_vos, total_count, minimal=self.get_minimal(params))
