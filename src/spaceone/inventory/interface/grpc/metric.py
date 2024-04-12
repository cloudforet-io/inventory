from spaceone.core.pygrpc import BaseAPI
from spaceone.api.inventory.v1 import metric_pb2, metric_pb2_grpc
from spaceone.inventory.service.metric_service import MetricService


class Metric(BaseAPI, metric_pb2_grpc.MetricServicer):
    pb2 = metric_pb2
    pb2_grpc = metric_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_svc = MetricService(metadata)
        response: dict = metric_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_svc = MetricService(metadata)
        response: dict = metric_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_svc = MetricService(metadata)
        metric_svc.delete(params)
        return self.empty()

    def run(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_svc = MetricService(metadata)
        metric_svc.run(params)
        return self.empty()

    def test(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_svc = MetricService(metadata)
        response: dict = metric_svc.test(params)
        return self.dict_to_message(response)

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_svc = MetricService(metadata)
        response: dict = metric_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_svc = MetricService(metadata)
        response: dict = metric_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_svc = MetricService(metadata)
        response: dict = metric_svc.stat(params)
        return self.dict_to_message(response)
