from spaceone.core.pygrpc import BaseAPI
from spaceone.api.inventory.v1 import metric_data_pb2, metric_data_pb2_grpc
from spaceone.inventory.service.metric_data_service import MetricDataService


class MetricData(BaseAPI, metric_data_pb2_grpc.MetricDataServicer):
    pb2 = metric_data_pb2
    pb2_grpc = metric_data_pb2_grpc

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_data_svc = MetricDataService(metadata)
        response: dict = metric_data_svc.list(params)
        return self.dict_to_message(response)

    def analyze(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_data_svc = MetricDataService(metadata)
        response: dict = metric_data_svc.analyze(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_data_svc = MetricDataService(metadata)
        response: dict = metric_data_svc.stat(params)
        return self.dict_to_message(response)
