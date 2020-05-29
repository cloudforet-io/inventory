from spaceone.api.inventory.v1 import region_pb2, region_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Region(BaseAPI, region_pb2_grpc.RegionServicer):

    pb2 = region_pb2
    pb2_grpc = region_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            return self.locator.get_info('RegionInfo', region_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            return self.locator.get_info('RegionInfo', region_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            region_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            return self.locator.get_info('RegionInfo', region_service.get(params))

    def add_member(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            return self.locator.get_info('RegionMemberInfo', region_service.add_member(params))

    def modify_member(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            return self.locator.get_info('RegionMemberInfo', region_service.modify_member(params))

    def remove_member(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            region_service.remove_member(params)
            return self.locator.get_info('EmptyInfo')

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            region_vos, total_count = region_service.list(params)
            return self.locator.get_info('RegionsInfo', region_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            return self.locator.get_info('StatisticsInfo', region_service.stat(params))

    def list_members(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('RegionService', metadata) as region_service:
            region_map_vo, total_count = region_service.list_members(params)
            return self.locator.get_info('RegionMembersInfo', region_map_vo, total_count)
