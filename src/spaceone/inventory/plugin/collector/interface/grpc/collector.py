from spaceone.core.pygrpc import BaseAPI
from spaceone.api.inventory.plugin import collector_pb2, collector_pb2_grpc
from spaceone.inventory.plugin.collector.service.collector_service import CollectorService


class Collector(BaseAPI, collector_pb2_grpc.CollectorServicer):

    pb2 = collector_pb2
    pb2_grpc = collector_pb2_grpc

    def init(self, request, context):
        params, metadata = self.parse_request(request, context)
        collector_svc = CollectorService(metadata)
        response: dict = collector_svc.init(params)
        return self.dict_to_message(response)

    def verify(self, request, context):
        params, metadata = self.parse_request(request, context)
        collector_svc = CollectorService(metadata)
        collector_svc.verify(params)
        return self.empty()

    def collect(self, request, context):
        params, metadata = self.parse_request(request, context)
        collector_svc = CollectorService(metadata)
        for response in collector_svc.collect(params):

            if response['state'] == 'FAILURE':
                response['resource_type'] = 'inventory.ErrorResource'

            if 'error_message' in response:
                error_message = response.pop('error_message')
                response['message'] = error_message

            if 'resource_data' in response:
                resource_data = response.pop('resource_data')
                response['resource'] = resource_data

            if 'match_keys' in response:
                match_keys = response.pop('match_keys')
                response['match_rules'] = {}

                for idx, keys in enumerate(match_keys, 1):
                    response['match_rules'][str(idx)] = keys

            yield self.dict_to_message(response)
