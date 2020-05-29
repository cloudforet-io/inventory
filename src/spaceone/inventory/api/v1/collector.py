from spaceone.api.inventory.v1 import collector_pb2, collector_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Collector(BaseAPI, collector_pb2_grpc.CollectorServicer):

    pb2 = collector_pb2
    pb2_grpc = collector_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('CollectorInfo', collector_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('CollectorInfo', collector_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            collector_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('CollectorInfo', collector_service.get(params))

    def enable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('CollectorInfo', collector_service.enable(params))

    def disable(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('CollectorInfo', collector_service.disable(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            collector_vos, total_count = collector_service.list(params)
            return self.locator.get_info('CollectorsInfo', collector_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('StatisticsInfo', collector_service.stat(params))

    def collect(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('JobInfo', collector_service.collect(params))

    def verify_plugin(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('VerifyInfo', collector_service.verify_plugin(params))

    def add_schedule(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('ScheduleInfo', collector_service.add_schedule(params))

    def get_schedule(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('ScheduleInfo', collector_service.get_schedule(params))

    def update_schedule(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            return self.locator.get_info('ScheduleInfo', collector_service.update_schedule(params))

    def delete_schedule(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            collector_service.delete_schedule(params)
            return self.locator.get_info('EmptyInfo')

    def list_schedules(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorService', metadata) as collector_service:
            schedule_vos, total_count = collector_service.list_schedules(params)
            return self.locator.get_info('SchedulesInfo', schedule_vos, total_count, minimal=self.get_minimal(params))

