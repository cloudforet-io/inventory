from spaceone.api.inventory.v1 import zone_pb2, zone_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Zone(BaseAPI, zone_pb2_grpc.ZoneServicer):

    pb2 = zone_pb2
    pb2_grpc = zone_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            return self.locator.get_info('ZoneInfo', zone_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            return self.locator.get_info('ZoneInfo', zone_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            zone_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            return self.locator.get_info('ZoneInfo', zone_service.get(params))

    def add_member(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            return self.locator.get_info('ZoneMemberInfo', zone_service.add_member(params))

    def modify_member(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            return self.locator.get_info('ZoneMemberInfo', zone_service.modify_member(params))

    def remove_member(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            zone_service.remove_member(params)
            return self.locator.get_info('EmptyInfo')

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            zone_vos, total_count = zone_service.list(params)
            return self.locator.get_info('ZonesInfo', zone_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            return self.locator.get_info('StatisticsInfo', zone_service.stat(params))

    def list_members(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ZoneService', metadata) as zone_service:
            zone_map_vo, total_count = zone_service.list_members(params)
            return self.locator.get_info('ZoneMembersInfo', zone_map_vo, total_count)

