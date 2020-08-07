from spaceone.api.inventory.v1 import job_task_pb2, job_task_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class JobTask(BaseAPI, job_task_pb2_grpc.JobTaskServicer):

    pb2 = job_task_pb2
    pb2_grpc = job_task_pb2_grpc

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('JobTaskService', metadata) as job_task_service:
            job_task_vos, total_count = job_task_service.list(params)
            return self.locator.get_info('JobTasksInfo', job_task_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('JobTaskService', metadata) as job_task_service:
            return self.locator.get_info('StatisticsInfo', job_task_service.stat(params))
