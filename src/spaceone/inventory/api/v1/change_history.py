from spaceone.api.inventory.v1 import change_history_pb2, change_history_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class ChangeHistory(BaseAPI, change_history_pb2_grpc.ChangeHistoryServicer):

    pb2 = change_history_pb2
    pb2_grpc = change_history_pb2_grpc

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ChangeHistoryService', metadata) as ch_service:
            record_vos, total_count = ch_service.list(params)
            return self.locator.get_info('ChangeHistoryInfo', record_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('ChangeHistoryService', metadata) as ch_service:
            return self.locator.get_info('StatisticsInfo', ch_service.stat(params))
