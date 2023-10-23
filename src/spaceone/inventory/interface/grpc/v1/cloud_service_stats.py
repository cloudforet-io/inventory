from spaceone.api.inventory.v1 import cloud_service_stats_pb2, cloud_service_stats_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class CloudServiceStats(BaseAPI, cloud_service_stats_pb2_grpc.CloudServiceStatsServicer):

    pb2 = cloud_service_stats_pb2
    pb2_grpc = cloud_service_stats_pb2_grpc

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceStatsService', metadata) as cloud_svc_stats_service:
            cloud_svc_stats_vos, total_count = cloud_svc_stats_service.list(params)
            return self.locator.get_info('CloudServiceStatsInfo',
                                         cloud_svc_stats_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def analyze(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceStatsService', metadata) as cloud_svc_stats_service:
            return self.locator.get_info('AnalyzeInfo', cloud_svc_stats_service.analyze(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceStatsService', metadata) as cloud_svc_stats_service:
            return self.locator.get_info('StatisticsInfo', cloud_svc_stats_service.stat(params))
