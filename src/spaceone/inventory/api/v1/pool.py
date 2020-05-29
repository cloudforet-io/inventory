from spaceone.api.inventory.v1 import pool_pb2, pool_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Pool(BaseAPI, pool_pb2_grpc.PoolServicer):

    pb2 = pool_pb2
    pb2_grpc = pool_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            return self.locator.get_info('PoolInfo', pool_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            return self.locator.get_info('PoolInfo', pool_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            pool_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            return self.locator.get_info('PoolInfo', pool_service.get(params))

    def add_member(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            return self.locator.get_info('PoolMemberInfo', pool_service.add_member(params))

    def modify_member(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            return self.locator.get_info('PoolMemberInfo', pool_service.modify_member(params))

    def remove_member(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            pool_service.remove_member(params)
            return self.locator.get_info('EmptyInfo')

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            pool_vos, total_count = pool_service.list(params)
            return self.locator.get_info('PoolsInfo', pool_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            return self.locator.get_info('StatisticsInfo', pool_service.stat(params))

    def list_members(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('PoolService', metadata) as pool_service:
            pool_vos, total_count = pool_service.list_members(params)
            return self.locator.get_info('PoolMembersInfo', pool_vos, total_count)