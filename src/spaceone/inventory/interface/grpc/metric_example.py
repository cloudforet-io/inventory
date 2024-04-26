from spaceone.core.pygrpc import BaseAPI
from spaceone.api.inventory.v1 import metric_example_pb2, metric_example_pb2_grpc
from spaceone.inventory.service.metric_example_service import MetricExampleService


class MetricExample(BaseAPI, metric_example_pb2_grpc.MetricExampleServicer):
    pb2 = metric_example_pb2
    pb2_grpc = metric_example_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_example_svc = MetricExampleService(metadata)
        response: dict = metric_example_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_example_svc = MetricExampleService(metadata)
        response: dict = metric_example_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_example_svc = MetricExampleService(metadata)
        metric_example_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_example_svc = MetricExampleService(metadata)
        response: dict = metric_example_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_example_svc = MetricExampleService(metadata)
        response: dict = metric_example_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        metric_example_svc = MetricExampleService(metadata)
        response: dict = metric_example_svc.stat(params)
        return self.dict_to_message(response)
