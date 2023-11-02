from spaceone.core.pygrpc import BaseAPI
from spaceone.api.inventory.plugin import job_pb2, job_pb2_grpc
from spaceone.inventory.plugin.collector.service.job_service import JobService


class Job(BaseAPI, job_pb2_grpc.JobServicer):

    pb2 = job_pb2
    pb2_grpc = job_pb2_grpc

    def get_tasks(self, request, context):
        params, metadata = self.parse_request(request, context)
        job_svc = JobService(metadata)
        response: dict = job_svc.get_tasks(params)
        return self.dict_to_message(response)
