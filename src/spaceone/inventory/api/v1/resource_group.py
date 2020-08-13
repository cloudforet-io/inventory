from spaceone.api.inventory.v1 import resource_group_pb2, resource_group_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class ResourceGroup(BaseAPI, resource_group_pb2_grpc.ResourceGroupServicer):

    pb2 = resource_group_pb2
    pb2_grpc = resource_group_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ResourceGroupService', metadata) as resource_group_service:
            return self.locator.get_info('ResourceGroupInfo', resource_group_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ResourceGroupService', metadata) as resource_group_service:
            return self.locator.get_info('ResourceGroupInfo', resource_group_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ResourceGroupService', metadata) as resource_group_service:
            resource_group_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ResourceGroupService', metadata) as resource_group_service:
            return self.locator.get_info('ResourceGroupInfo', resource_group_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ResourceGroupService', metadata) as resource_group_service:
            resource_group_vos, total_count = resource_group_service.list(params)
            return self.locator.get_info('ResourceGroupsInfo', resource_group_vos, total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ResourceGroupService', metadata) as resource_group_service:
            return self.locator.get_info('StatisticsInfo', resource_group_service.stat(params))
