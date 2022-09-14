from spaceone.api.inventory.v1 import cloud_service_tag_pb2, cloud_service_tag_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class CloudServiceTag(BaseAPI, cloud_service_tag_pb2_grpc.CloudServiceTagServicer):

    pb2 = cloud_service_tag_pb2
    pb2_grpc = cloud_service_tag_pb2_grpc

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceTagService', metadata) as cloud_svc_tag_service:
            cloud_svc_tag_vos, total_count = cloud_svc_tag_service.list(params)
            return self.locator.get_info('CloudServiceTagsInfo',
                                         cloud_svc_tag_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CloudServiceTagService', metadata) as cloud_svc_tag_service:
            return self.locator.get_info('StatisticsInfo', cloud_svc_tag_service.stat(params))
