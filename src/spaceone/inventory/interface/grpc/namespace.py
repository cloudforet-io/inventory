from spaceone.core.pygrpc import BaseAPI
from spaceone.api.inventory.v1 import namespace_pb2, namespace_pb2_grpc
from spaceone.inventory.service.namespace_service import NamespaceService


class Namespace(BaseAPI, namespace_pb2_grpc.NamespaceServicer):
    pb2 = namespace_pb2
    pb2_grpc = namespace_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)
        namespace_svc = NamespaceService(metadata)
        response: dict = namespace_svc.create(params)
        return self.dict_to_message(response)

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)
        namespace_svc = NamespaceService(metadata)
        response: dict = namespace_svc.update(params)
        return self.dict_to_message(response)

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)
        namespace_svc = NamespaceService(metadata)
        namespace_svc.delete(params)
        return self.empty()

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)
        namespace_svc = NamespaceService(metadata)
        response: dict = namespace_svc.get(params)
        return self.dict_to_message(response)

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)
        namespace_svc = NamespaceService(metadata)
        response: dict = namespace_svc.list(params)
        return self.dict_to_message(response)

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)
        namespace_svc = NamespaceService(metadata)
        response: dict = namespace_svc.stat(params)
        return self.dict_to_message(response)
