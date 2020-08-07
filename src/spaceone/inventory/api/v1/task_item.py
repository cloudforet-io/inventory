from spaceone.api.inventory.v1 import task_item_pb2, task_item_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class TaskItem(BaseAPI, task_item_pb2_grpc.TaskItemServicer):

    pb2 = task_item_pb2
    pb2_grpc = task_item_pb2_grpc

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('TaskItemService', metadata) as task_item_service:
            task_item_vos, total_count = task_item_service.list(params)
            return self.locator.get_info('TaskItemsInfo', task_item_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('TaskItemService', metadata) as task_item_service:
            return self.locator.get_info('StatisticsInfo', task_item_service.stat(params))
